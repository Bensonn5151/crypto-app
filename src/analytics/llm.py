from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.docstore.document import Document
import json
import traceback
from groq import Groq
import os
import sys

# === PATH CONFIG ===
json_path = "/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app/coindesk_news_latest.json"
save_path = "/Users/apple/Desktop/DEV/PORTFOLIO/crypto-app/faiss_db"  # absolute, consistent

# === CHECK FILES & KEYS EARLY ===
if not os.path.exists(json_path):
    print(f"❌ JSON file not found at: {json_path}")
    sys.exit(1)

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    print("❌ GROQ_API_KEY not set in environment. Please export GROQ_API_KEY and retry.")
    sys.exit(1)

# (Don't overwrite the env var with None)
os.environ["GROQ_API_KEY"] = GROQ_API_KEY

# === LOAD JSON DATA ===
with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)

articles = data.get("Data", [])
docs = []

# === BUILD DOCUMENT OBJECTS ===
for article in articles:
    title = article.get("TITLE", "")
    body = article.get("BODY", "")
    keywords = article.get("KEYWORDS", "")
    author = article.get("AUTHORS", "")
    source = article.get("SOURCE_DATA", {}).get("NAME", "")
    url = article.get("URL", "")

    # Combine content fields into one text blob
    text = f"Title: {title}\nAuthor: {author}\nSource: {source}\nKeywords: {keywords}\n\n{body}\n\nURL: {url}"
    
    if text.strip():
        docs.append(Document(
            page_content=text,
            metadata={
                "title": title,
                "author": author,
                "source": source,
                "url": url,
                "keywords": keywords
            }
        ))

print(f"📰 Loaded {len(docs)} articles from JSON")

# === SPLIT INTO CHUNKS ===
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000,
    chunk_overlap=200
)
texts = text_splitter.split_documents(docs)
print(f"✂️ Split into {len(texts)} text chunks")

# === INITIALIZE EMBEDDINGS ===
# NOTE: make sure sentence-transformers + torch are installed and importable in your venv
try:
    embedding = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
except Exception as e:
    print("❌ Failed to initialize HuggingFaceEmbeddings. Error:")
    traceback.print_exc()
    sys.exit(1)

# === CREATE or LOAD FAISS VECTOR STORE ===
os.makedirs(save_path, exist_ok=True)
faiss_exists = os.path.exists(os.path.join(save_path, "index.faiss")) or os.path.exists(save_path)

if not faiss_exists or os.listdir(save_path) == []:
    print("🚀 Creating FAISS database from documents...")
    try:
        vectordb = FAISS.from_documents(texts, embedding)
        vectordb.save_local(save_path)
        print(f"✅ FAISS database created and saved to: {save_path}")
    except Exception as e:
        print("❌ Failed to create/save FAISS DB:")
        traceback.print_exc()
        sys.exit(1)
else:
    print("📥 Loading existing FAISS database...")
    try:
        vectordb = FAISS.load_local(save_path, embedding, allow_dangerous_deserialization=True)
        # ntotal may be under vectordb.index or vectordb.index_to_docstore_id depending on FAISS wrapper
        try:
            total = getattr(vectordb.index, "ntotal", None)
        except Exception:
            total = None
        print(f"✅ Loaded FAISS DB. n_vectors = {total}")
    except Exception as e:
        print("❌ Failed to load FAISS DB:")
        traceback.print_exc()
        sys.exit(1)

# Initialize Groq client
try:
    client = Groq()  # Groq uses GROQ_API_KEY from env
    print("✅ Groq client initialized successfully")
except Exception as e:
    print("❌ Failed to initialize Groq client. Error:")
    traceback.print_exc()
    sys.exit(1)

def get_llm_response(messages, temperature=0.5, max_tokens=512, stream=False):
    """Get response from Groq LLM using direct API call"""
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=1,
            stream=stream,
            stop=None
        )
        
        if stream:
            response = ""
            for chunk in completion:
                # defensive checks for chunk structure
                try:
                    delta = chunk.choices[0].delta
                    if getattr(delta, "content", None):
                        response += delta.content
                except Exception:
                    continue
            return response
        else:
            return completion.choices[0].message.content
            
    except Exception as e:
        print(f"❌ Error calling Groq API: {e}")
        traceback.print_exc()
        return None

def get_retrieved_context(question, k=3):
    """Retrieve relevant documents from FAISS"""
    try:
        docs = vectordb.similarity_search(question, k=k)
        context = "\n\n".join([doc.page_content for doc in docs])
        source_docs = docs
        return context, source_docs
    except Exception as e:
        print(f"❌ Error retrieving documents: {e}")
        traceback.print_exc()
        return "", []

print("🤖 Chatbot is ready! Type 'quit' to exit.\n")

# Simple chat interface
while True:
    question = input("You: ")
    
    if question.lower() in ['quit', 'exit', 'bye']:
        print("Goodbye! 👋")
        break
    
    if not question.strip():
        continue
    
    try:
        # Step 1: Retrieve relevant documents
        print("🔍 Searching documents...")
        context, source_docs = get_retrieved_context(question, k=3)
        
        # Step 2: Create prompt with context
        prompt = f"""Based on the following context, please answer the question. If the context doesn't contain relevant information, please say so.

Context:
{context}

Question: {question}

Answer:"""
        
        # Step 3: Get LLM response
        messages = [
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        print("🤖 Generating response...")
        answer = get_llm_response(messages, temperature=0.5, max_tokens=512, stream=False)
        
        if answer:
            print(f"\n🤖 Bot: {answer}\n")
            
            # Show which chunks were used
            if source_docs:
                print("📚 Sources used:")
                for i, doc in enumerate(source_docs, 1):
                    preview = doc.page_content[:150].replace('\n', ' ')
                    print(f"  {i}. {preview}...")
                    if doc.metadata:
                        source_file = doc.metadata.get('source', 'Unknown')
                        print(f"     📄 From: {source_file}")
                print()
        else:
            print("❌ Failed to get response from LLM\n")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}\n")
        print("Full traceback:")
        traceback.print_exc()
        print("\nPlease try rephrasing your question.\n")