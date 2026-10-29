import os
import sys
import json
import hashlib
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import pypdf
from sqlalchemy import text

from langchain_openai import OpenAIEmbeddings
from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
from langchain_core.documents import Document

# Suppress the Windows symlink warning noise completely
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "true"

# ==========================================
# 1. PATH RESOLUTION & SETUP
# ==========================================
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

from src.db import get_engine

# Admin login: only the admin may write SOP chunks (the agent can only read them)
engine = get_engine(os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PASSWORD"))

# ==========================================
# 2. DYNAMIC ENVIRONMENT ROUTING
# ==========================================
EMBEDDINGS_MODEL_SETTING = os.getenv("Embeddings_model", "LOCAL").strip().upper()

if EMBEDDINGS_MODEL_SETTING == "OPENAI":
    print("🤖 Mode: Utilizing Cloud OpenAI Embeddings (1536 Dim)...")
    embeddings = OpenAIEmbeddings()
else:
    local_model_target = os.getenv("Local_Embedding_Model", "BAAI/bge-m3").strip()
    print(f"🤗 Mode: Local Embeddings [{local_model_target}] (1024 Dim)...")
    from langchain_huggingface import HuggingFaceEmbeddings
    embeddings = HuggingFaceEmbeddings(model_name=local_model_target, model_kwargs={'device': 'cpu'})

# ==========================================
# 3. ROBUST POLYMORPHIC PARSER
# ==========================================
def parse_and_chunk_document(doc_path: Path) -> list[Document]:
    ext = doc_path.suffix.lower()
    raw_chunks: list[Document] = []
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=600, chunk_overlap=60)
    
    if ext == ".md":
        headers_to_split_on = [("#", "Header_1"), ("##", "Header_2"), ("###", "Header_3")]
        md_splitter = MarkdownHeaderTextSplitter(headers_to_split_on=headers_to_split_on)
        raw_text = doc_path.read_text(encoding="utf-8")
        header_docs = md_splitter.split_text(raw_text)
        raw_chunks = text_splitter.split_documents(header_docs)
        
    elif ext == ".txt":
        raw_text = doc_path.read_text(encoding="utf-8")
        raw_docs = [Document(page_content=raw_text)]
        raw_chunks = text_splitter.split_documents(raw_docs)
        
    elif ext == ".pdf":
        pdf_docs = []
        try:
            with open(doc_path, "rb") as f:
                reader = pypdf.PdfReader(f)
                for page_num, page in enumerate(reader.pages):
                    page_text = page.extract_text()
                    if page_text and page_text.strip():
                        pdf_docs.append(Document(page_content=page_text, metadata={"page_number": page_num + 1}))
            raw_chunks = text_splitter.split_documents(pdf_docs)
        except Exception as e:
            print(f"  ❌ Error parsing PDF {doc_path.name}: {e}")
            return []
        
    elif ext in [".csv", ".xlsx"]:
        try:
            df = pd.read_csv(doc_path) if ext == ".csv" else pd.read_excel(doc_path)
        except Exception as e:
            print(f"  ❌ Error reading table: {e}")
            return []
            
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            row_items = [
                f"{str(col)}: {str(val)}" 
                for col, val in row_dict.items() 
                if pd.notna(val) and str(val).strip() != ""
            ]
            
            if row_items:
                row_text = " | ".join(row_items)
                doc_item = Document(page_content=row_text, metadata={"row_index": int(idx)})
                raw_chunks.append(doc_item)

    # sanity check
    valid_chunks = []
    for chunk in raw_chunks:
        clean_text = chunk.page_content.strip()
        if clean_text:
            chunk.page_content = clean_text
            valid_chunks.append(chunk)
            
    return valid_chunks

# ==========================================
# 4. INCREMENTAL PIPELINE (pgvector)
#    Each chunk stores its file's MD5 in metadata, so the database itself
#    knows what is already ingested (no separate cache file to drift out of sync).
# ==========================================
policy_dir = project_root / "data" / "policy"
target_patterns = ["*.md", "*.txt", "*.pdf", "*.csv", "*.xlsx"]
current_files = {f.name: f for pattern in target_patterns for f in policy_dir.glob(pattern)}
print(f"Found {len(current_files)} policy file(s) in {policy_dir}...")

with engine.begin() as conn:
    # Purge chunks of files that were deleted from data/policy
    conn.execute(text("DELETE FROM sop_chunks WHERE NOT (source_file = ANY(:names))"),
                 {"names": list(current_files)})

    for file_name, file_path in current_files.items():
        file_hash = hashlib.md5(file_path.read_bytes()).hexdigest()

        already_ingested = conn.execute(text(
            "SELECT 1 FROM sop_chunks WHERE source_file = :f AND metadata->>'file_hash' = :h LIMIT 1"
        ), {"f": file_name, "h": file_hash}).first()
        if already_ingested:
            print(f"✨ Skipped (Unchanged): {file_name}")
            continue

        print(f"🔄 Processing updates: {file_name}...")
        conn.execute(text("DELETE FROM sop_chunks WHERE source_file = :f"), {"f": file_name})

        chunks = parse_and_chunk_document(file_path)
        if not chunks:
            print(f"  ⚠️ No valid text chunks extracted from {file_name}.")
            continue

        vectors = embeddings.embed_documents([c.page_content for c in chunks])
        for idx, (chunk, vector) in enumerate(zip(chunks, vectors)):
            chunk.metadata.update({
                "source_file": file_name,
                "file_format": file_path.suffix.replace(".", "").upper(),
                "file_hash": file_hash,
            })
            conn.execute(text("""
                INSERT INTO sop_chunks (id, source_file, content, metadata, embedding)
                VALUES (:id, :f, :content, CAST(:meta AS JSONB), CAST(:emb AS VECTOR))
            """), {
                "id": f"{file_name}-chunk-{idx}",
                "f": file_name,
                "content": chunk.page_content,
                "meta": json.dumps(chunk.metadata),
                "emb": str(vector),  # pgvector accepts the text form '[0.1, 0.2, ...]'
            })
        print(f"  📤 Stored {len(chunks)} chunk(s).")

print("✅ SOP ingestion complete.")
