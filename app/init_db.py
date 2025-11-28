from app.db import init_db

print("🔧 Creating tables in PostgreSQL...")
init_db()
print("✅ Database tables created successfully!")
