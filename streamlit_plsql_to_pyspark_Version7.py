import os
import io
import tempfile
import subprocess
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from typing import List, Optional
import sqlparse
from plsql_chunker import split_plsql_into_blocks

# ──────────────── ENV & API LOADERS ────────────────
def load_env_from_session(session_state):
    # Use session_state or .env for API credentials
    env = {}
    # Gemini
    env["GEMINI_API_KEY"] = session_state.get("gemini_api_key") or os.getenv("GEMINI_API_KEY")
    # Azure OpenAI
    env["OPENAI_API_KEY"] = session_state.get("openai_api_key") or os.getenv("OPENAI_API_KEY")
    env["OPENAI_API_BASE"] = session_state.get("openai_api_base") or os.getenv("OPENAI_API_BASE")
    env["OPENAI_API_TYPE"] = session_state.get("openai_api_type") or os.getenv("OPENAI_API_TYPE", "azure")
    env["OPENAI_API_VERSION"] = session_state.get("openai_api_version") or os.getenv("OPENAI_API_VERSION")
    env["DEPLOYMENT_NAME"] = session_state.get("deployment_name") or os.getenv("DEPLOYMENT_NAME")
    env["MODEL_NAME"] = session_state.get("model_name") or os.getenv("MODEL_NAME", "gpt-4o")
    return env

# ──────────────── LLM PROVIDERS (Strategy Pattern) ────────────────
class LLMProvider:
    def convert(self, block: str) -> str:
        raise NotImplementedError

class GeminiProvider(LLMProvider):
    def __init__(self, api_key: str):
        from google.generativeai import configure as gemini_configure, GenerativeModel
        gemini_configure(api_key=api_key)
        self.model = GenerativeModel("gemini-1.5-pro")
    def convert(self, block: str) -> str:
        prompt = (
            "You are a senior data engineer experienced in migrating legacy PL/SQL code to PySpark.\n\n"
            "Convert the following PL/SQL block into clean, production-ready PySpark using the DataFrame API.\n"
            "Your output MUST:\n"
            "- Retain business logic and variable/column naming.\n"
            "- Use idiomatic PySpark (no unnecessary .rdd or UDF unless unavoidable).\n"
            "- Implement control structures (IF, WHILE, LOOP, EXCEPTION) using native Python logic where applicable.\n"
            "- Translate SQL operations (SELECT, JOIN, WHERE, GROUP BY) to PySpark.\n"
            "- Avoid comments, explanations, or markdown. Return only executable Python code.\n\n"
            f"PL/SQL Block:\n{block}\n"
        )
        try:
            resp = self.model.generate_content(prompt)
            return resp.text.strip()
        except Exception as e:
            return f"# Gemini Error: {e}"

class OpenAIProvider(LLMProvider):
    def __init__(self, api_key, api_base, api_type, api_version, deployment_name):
        import openai
        self.openai = openai
        openai.api_key = api_key
        openai.api_base = api_base
        openai.api_type = api_type
        openai.api_version = api_version
        self.deployment_name = deployment_name
    def convert(self, block: str) -> str:
        prompt = (
            "You are a data engineer. Convert the following PL/SQL code block into PySpark DataFrame API code.\n"
            "Only return valid, executable Python code. Do not include explanations, comments, or markdown.\n"
            f"PL/SQL Block:\n{block}\n"
        )
        try:
            resp = self.openai.ChatCompletion.create(
                engine=self.deployment_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            return f"# OpenAI Error: {e}"

def get_llm_provider(choice: str, env) -> Optional[LLMProvider]:
    if choice == "Gemini" and env["GEMINI_API_KEY"]:
        return GeminiProvider(env["GEMINI_API_KEY"])
    elif choice == "Azure OpenAI" and env["OPENAI_API_KEY"]:
        return OpenAIProvider(
            env["OPENAI_API_KEY"], env["OPENAI_API_BASE"], env["OPENAI_API_TYPE"],
            env["OPENAI_API_VERSION"], env["DEPLOYMENT_NAME"]
        )
    return None

# ──────────────── LINTING ────────────────
def lint_code(code: str) -> str:
    try:
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".py") as f:
            f.write(code)
            temp_path = f.name
        result = subprocess.run(["flake8", temp_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        os.unlink(temp_path)
        return result.stdout.decode("utf-8") or "✅ No lint issues found."
    except Exception as e:
        return f"⚠️ Linting failed: {e}"

# ──────────────── FAKE USER PROFILE ────────────────
def show_fake_user_profile():
    st.markdown(
        """
        <style>
        .profile-card {
            background: linear-gradient(135deg, #0f2027 0%, #2c5364 100%);
            color: #FFD700;
            border-radius: 18px;
            padding: 18px 16px 16px 16px;
            margin-bottom: 30px;
            box-shadow: 0 6px 24px 0 rgba(32,40,80,0.2);
            display: flex;
            align-items: center;
            gap: 16px;
        }
        .profile-avatar {
            border-radius: 50%;
            border: 3px solid #FFD700;
            width: 68px;
            height: 68px;
            object-fit: cover;
            box-shadow: 0 2px 8px #1a2236AA;
        }
        .profile-info {
            display: flex;
            flex-direction: column;
        }
        .profile-name {
            font-size: 1.13rem;
            font-weight: 700;
            color: #FFD700;
            margin-bottom: 2px;
        }
        .profile-role {
            font-size: 0.98rem;
            color: #e9e3c9;
            font-weight: 400;
        }
        </style>
        <div class="profile-card">
            <img src="https://ui-avatars.com/api/?name=Rahul+Chavan&background=2c5364&color=ffd700&size=128" class="profile-avatar" />
            <div class="profile-info">
                <span class="profile-name">Rahul Chavan</span>
                <span class="profile-role">🪄 Data Engineer</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

# ──────────────── STREAMLIT UI ────────────────

st.set_page_config(page_title="PL/SQL to PySpark • Rich UI", layout="wide")

# Custom global background and luxury effect
st.markdown("""
    <style>
    body {
        background: linear-gradient(120deg,#162447 0%,#1f4068 100%);
    }
    .block-container {
        background: linear-gradient(120deg,#162447 0%,#1f4068 100%);
        padding-bottom: 16px !important;
    }
    .stApp {
        background: linear-gradient(120deg,#162447 0%,#1f4068 100%) !important;
    }
    .stButton>button {
        color: #fff !important;
        background: linear-gradient(90deg, #FFD700 0%, #FFA500 100%) !important;
        border: none;
        border-radius: 6px;
        font-weight: 600;
        padding: 0.55em 1.2em !important;
        box-shadow: 0 2px 12px #FFD70044;
        margin-bottom: 4px;
    }
    .stTextArea textarea, .stTextInput input, .stFileUploader label {
        background: #f5f3d9 !important;
        color: #29335c !important;
        border-radius: 10px !important;
        border: 1.5px solid #FFD70033 !important;
        font-family: 'Fira Mono', monospace !important;
    }
    .stCode {
        background: #1f4068 !important;
        color: #FFD700 !important;
        border-radius: 12px !important;
        border: 1px solid #FFD70033 !important;
    }
    .stTable, .stDataFrame, .stMarkdown {
        background: rgba(255,255,255,0.03) !important;
        border-radius: 14px !important;
        border: 0.5px solid #FFD70033 !important;
    }
    .stDownloadButton>button {
        background: linear-gradient(90deg, #FFD700 0%, #FFA500 100%) !important;
        color: #1f4068 !important;
        border-radius: 6px;
        font-weight: 600;
        box-shadow: 0 2px 12px #FFD70044;
    }
    </style>
""", unsafe_allow_html=True)

st.title("🔄 <span style='color:#FFD700;'>PL/SQL</span> to <span style='color:#FFD700;'>PySpark</span> Converter <span style='font-size:0.7em;color:#FFD700;'>✨ Luxury UI</span>", unsafe_allow_html=True)

# Sidebar with luxury effect
with st.sidebar:
    show_fake_user_profile()
    st.markdown("<hr style='border:1px solid #FFD70033; margin:8px 0;'/>", unsafe_allow_html=True)
    st.header("⚙️ <span style='color:#FFD700'>Settings</span>", unsafe_allow_html=True)
    llm_choice = st.radio("Choose LLM Provider", ["Gemini", "Azure OpenAI"])
    input_method = st.radio("Input Method", ["Upload .sql File", "Paste Code"])
    enable_lint = st.checkbox("🔍 Lint Final PySpark Output", value=True)
    st.markdown("<hr style='border:1px solid #FFD70033; margin:8px 0;'/>", unsafe_allow_html=True)
    st.markdown("### <span style='color:#FFD700'>🔐 API Key Status</span>", unsafe_allow_html=True)

    # Toggle to enable/disable custom API entry
    manual_api = st.toggle("🔑 Enter API Credentials Manually", value=False)
    if manual_api:
        st.info("Credentials entered here will override .env.")
        st.session_state["gemini_api_key"] = st.text_input("Gemini API Key", type="password")
        st.session_state["openai_api_key"] = st.text_input("OpenAI API Key", type="password")
        st.session_state["openai_api_base"] = st.text_input("OpenAI API Base", value="https://your-azure-openai-resource.openai.azure.com/")
        st.session_state["openai_api_type"] = st.text_input("OpenAI API Type", value="azure")
        st.session_state["openai_api_version"] = st.text_input("OpenAI API Version", value="2023-05-15")
        st.session_state["deployment_name"] = st.text_input("Deployment Name")
        st.session_state["model_name"] = st.text_input("Model Name", value="gpt-4o")
    env = load_env_from_session(st.session_state)

    st.success("✅ Gemini API Loaded" if env["GEMINI_API_KEY"] else "❌ Gemini API Missing")
    st.success("✅ OpenAI API Loaded" if env["OPENAI_API_KEY"] else "❌ OpenAI API Missing")

# Example/test code
def example_plsql():
    return """CREATE OR REPLACE PROCEDURE update_salary IS
  v_count NUMBER := 0;
BEGIN
  SELECT COUNT(*) INTO v_count FROM employees WHERE department_id = 10;
  IF v_count > 0 THEN
    UPDATE employees SET salary = salary * 1.1 WHERE department_id = 10;
  END IF;
END;
/

-- Standalone statement
UPDATE departments SET location_id = 2000 WHERE department_id = 20;

CREATE OR REPLACE FUNCTION get_department_name(dept_id NUMBER) RETURN VARCHAR2 IS
  dept_name VARCHAR2(50);
BEGIN
  SELECT department_name INTO dept_name FROM departments WHERE department_id = dept_id;
  RETURN dept_name;
END;
/
"""

st.markdown(
    "<div style='margin-bottom:14px'><button style='background:linear-gradient(90deg,#FFD700,#FFA500);color:#1f4068;border:none;border-radius:10px;font-weight:700;padding:7px 18px;box-shadow:0 2px 10px #FFD70033;cursor:pointer;' onclick='window.location.reload()'>🪄 Load Example PL/SQL</button></div>",
    unsafe_allow_html=True
)
if st.button("Load Example PL/SQL"):
    st.session_state["example_sql"] = example_plsql()

# Get input
sql_code = ""
if input_method == "Upload .sql File":
    uploaded_file = st.file_uploader("Upload PL/SQL file", type=["sql", "txt"])
    if uploaded_file:
        sql_code = uploaded_file.read().decode("utf-8")
else:
    sql_code = st.text_area("Paste PL/SQL code here", height=300,
                            value=st.session_state.get("example_sql", ""))

if sql_code:
    st.markdown("<div style='font-size:1.2em;font-weight:600;color:#FFD700;margin:10px 0 0 0;'>📄 Original PL/SQL Code</div>", unsafe_allow_html=True)
    st.code(sql_code, language="sql")

    # --- Advanced robust parsing here ---
    blocks = split_plsql_into_blocks(sql_code, max_chunk_size=1200)
    provider = get_llm_provider(llm_choice, env)
    if provider is None:
        st.error("❌ LLM provider not properly configured. Check your API credentials.")
        st.stop()

    # Session state for conversions
    if "converted_blocks" not in st.session_state or st.session_state.get("sql_snapshot") != sql_code:
        st.session_state["converted_blocks"] = [None] * len(blocks)
        st.session_state["sql_snapshot"] = sql_code

    # Convert each chunk (with progress bar, allow re-run per chunk)
    converted_blocks = st.session_state["converted_blocks"]
    st.markdown("<div style='font-size:1.15em;font-weight:500;color:#FFD700;margin:22px 0 0 0;'>🔄 Convert Blocks</div>", unsafe_allow_html=True)
    for i, block in enumerate(blocks):
        col1, col2 = st.columns([3, 5])
        with col1:
            st.markdown(f"<div style='color:#FFD700;font-weight:600;'>Block {i+1}</div>", unsafe_allow_html=True)
            st.code(block, language="sql")
            if st.button(f"Convert Block {i+1}", key=f"convert_{i}"):
                with st.spinner("Converting..."):
                    converted_blocks[i] = provider.convert(block)
        with col2:
            if converted_blocks[i]:
                st.code(converted_blocks[i], language="python")
            else:
                st.info("Not converted yet.")

    # Merge output
    all_converted = [cb for cb in converted_blocks if cb]
    final_output = "\n\n# ───── Next Block ─────\n\n".join(all_converted) if all_converted else ""
    if final_output:
        st.markdown("<div style='font-size:1.15em;font-weight:500;color:#FFD700;margin:22px 0 0 0;'>🐍 Final PySpark Code</div>", unsafe_allow_html=True)
        st.code(final_output, language="python")
        st.download_button("📥 Download PySpark Code", final_output, file_name="converted_pyspark.py")

        if enable_lint:
            st.markdown("<div style='font-size:1.09em;font-weight:500;color:#FFD700;margin:20px 0 0 0;'>🧹 Linting Result</div>", unsafe_allow_html=True)
            lint_result = lint_code(final_output)
            st.text(lint_result)

        # Preview Table
        st.markdown("<div style='font-size:1.09em;font-weight:500;color:#FFD700;margin:20px 0 0 0;'>🧾 Preview: PL/SQL Block vs PySpark</div>", unsafe_allow_html=True)
        preview_df = pd.DataFrame({
            "PL/SQL Block": blocks,
            "Converted PySpark": converted_blocks
        })
        st.dataframe(preview_df, use_container_width=True)
        # Download as CSV
        csv_buffer = io.StringIO()
        preview_df.to_csv(csv_buffer, index=False)
        st.download_button("📥 Download PL/SQL Blocks (.csv)", data=csv_buffer.getvalue(),
                          file_name="plsql_blocks.csv", mime="text/csv")
    else:
        st.warning("Convert at least one block to see the results!")
else:
    st.info("Upload a file or paste PL/SQL code to begin.")