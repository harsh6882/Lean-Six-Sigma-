import os
import pickle
import hashlib
import logging
from datetime import datetime
import pandas as pd
import streamlit as st

# ==========================================
# 1. EXCEPTIONS & OBSERVER
# ==========================================
class DefectAlreadyResolvedException(Exception): pass
class CriticalHaltException(Exception): pass

# ==========================================
# 2. SECURITY & UTILITIES 
# ==========================================
class SecurityUtils:
    @staticmethod
    def hash_password(password):
        return hashlib.sha256(password.encode('utf-8')).hexdigest()

class ConfigManager:
    CONFIG_FILE = "application.json"

    def __init__(self):
        self.props = {}
        if not os.path.exists(self.CONFIG_FILE):
            self._create_default_config()
        else:
            self._load_config()

    def _create_default_config(self):
        import json
        self.props = {
            "db.url": "jdbc:mysql://localhost:3306/six_sigma",
            "db.user": "root",
            "db.password": "password",
            "admin.hash": SecurityUtils.hash_password("sigma123")
        }
        with open(self.CONFIG_FILE, 'w') as f:
            json.dump(self.props, f, indent=4)

    def _load_config(self):
        import json
        try:
            with open(self.CONFIG_FILE, 'r') as f:
                self.props = json.load(f)
        except Exception:
            self._create_default_config()

    def get_property(self, key):
        return self.props.get(key)

# ==========================================
# 3. ABSTRACT CLASSES & MODELS
# ==========================================
class Defect:
    def __init__(self, defect_id, task_name, description, logged_by):
        self.id = defect_id
        self.task_name = task_name
        self.description = description
        self.logged_by = logged_by
        self.is_resolved = False
        self.resolved_by = "None"
        self.resolution_details = "Pending"
        self.logged_at = datetime.now()
        self.resolved_at = None

    def resolve(self, resolver_name, details):
        if self.is_resolved:
            raise DefectAlreadyResolvedException("Defect already resolved.")
        self.is_resolved = True
        self.resolved_by = resolver_name
        self.resolution_details = details
        self.resolved_at = datetime.now()

    def get_formatted_date(self):
        return self.logged_at.strftime("%d-%b %H:%M")
    
    def get_aging(self):
        end_time = self.resolved_at if self.is_resolved else datetime.now()
        duration = end_time - self.logged_at
        hours, remainder = divmod(duration.total_seconds(), 3600)
        minutes, _ = divmod(remainder, 60)
        return f"{int(hours)}h {int(minutes)}m"

    def get_impact_level(self): raise NotImplementedError
    def get_priority_weight(self): raise NotImplementedError

    def __lt__(self, other):
        if self.is_resolved and not other.is_resolved: return False
        if not self.is_resolved and other.is_resolved: return True
        
        priority_diff = other.get_priority_weight() - self.get_priority_weight()
        if priority_diff != 0:
            return priority_diff < 0 
        return self.logged_at > other.logged_at

class MinorDefect(Defect):
    def __init__(self, defect_id, task_name, desc, user, cosmetic_area):
        super().__init__(defect_id, task_name, desc, user)
        self.cosmetic_area = cosmetic_area
    def get_impact_level(self): return "Minor"
    def get_priority_weight(self): return 1

class CriticalDefect(Defect):
    def __init__(self, defect_id, task_name, desc, user, safety_risk):
        super().__init__(defect_id, task_name, desc, user)
        self.safety_risk = safety_risk
    def get_impact_level(self): return "CRITICAL"
    def get_priority_weight(self): return 10

# ==========================================
# 4. SMART RESOLUTION ENGINE
# ==========================================
class ResolutionEngine:
    @staticmethod
    def suggest_fix(defect):
        desc = defect.description.lower()
        if any(word in desc for word in ["wire", "voltage", "shock", "electric"]):
            return "Isolate power supply immediately. Replace damaged wiring, perform continuity testing, and verify against safety protocol IEEE-1584."
        elif any(word in desc for word in ["scratch", "paint", "dent", "cosmetic"]):
            return "Route to touch-up station. Buff affected area, reapply surface coating, and verify thickness under QA lighting."
        elif any(word in desc for word in ["missing", "screw", "bolt", "part"]):
            return "Perform line-stop. Replenish hardware bins, install missing components, and recalibrate automated dispensers."
        elif any(word in desc for word in ["software", "crash", "bug", "boot"]):
            return "Extract error logs, flash firmware to the last known stable version, and reboot system in diagnostic mode."
        elif defect.get_impact_level() == "CRITICAL":
            return "Halt operation immediately. Escalate to Senior Engineering for a 5-Whys Root Cause Analysis (RCA) before resuming."
        else:
            return "Apply standard Tier-1 rework procedure and flag unit for secondary QA inspection."

class DefectFactory:
    @staticmethod
    def create_defect(defect_type, defect_id, task, desc, user, detail):
        if defect_type.lower() == "critical":
            return CriticalDefect(defect_id, task, desc, user, detail)
        return MinorDefect(defect_id, task, desc, user, detail)

# ==========================================
# 5. DATA LOG 
# ==========================================
class DefectLog:
    def __init__(self):
        self.defects = []
    def add_defect(self, defect): self.defects.append(defect)
    def get_defects(self): return self.defects
    def find_defect(self, defect_id):
        for defect in self.defects:
            if defect.id.lower() == defect_id.lower():
                return defect
        return None
    def remove_resolved_defects(self):
        self.defects = [d for d in self.defects if not d.is_resolved]

# ==========================================
# 6. CONTROLLER (Adapted for Web Sessions)
# ==========================================
class TrackerController:
    DATA_FILE = "database.pkl"
    LOGGER = logging.getLogger("TrackerController")

    def __init__(self):
        self.setup_logger()
        self.config = ConfigManager()
        self.load_data()

    def setup_logger(self):
        handler = logging.FileHandler("system_audit.log")
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.LOGGER.addHandler(handler)
        self.LOGGER.setLevel(logging.INFO)

    def report_defect(self, defect):
        self.log_database.add_defect(defect)
        self.force_save_data()
        self.LOGGER.info(f"New defect logged: {defect.id}")
        if isinstance(defect, CriticalDefect):
            raise CriticalHaltException(f"!!! SYSTEM HALT !!!\nCRITICAL DEFECT LOGGED: {defect.description}")

    def process_resolution(self, defect_id, manager_name, details):
        defect = self.log_database.find_defect(defect_id)
        if defect:
            defect.resolve(manager_name, details)
            self.force_save_data()
        else:
            raise ValueError(f"Error: Defect ID '{defect_id}' not found.")

    def clear_resolved_defects(self):
        initial_count = len(self.log_database.get_defects())
        self.log_database.remove_resolved_defects()
        if initial_count != len(self.log_database.get_defects()):
            self.force_save_data()
        else:
            raise RuntimeError("No resolved defects found to clear.")

    def force_save_data(self):
        try:
            with open(self.DATA_FILE, 'wb') as f:
                pickle.dump(self.log_database, f)
        except Exception as e:
            self.LOGGER.error(f"Failed to save database state: {e}")

    def load_data(self):
        try:
            with open(self.DATA_FILE, 'rb') as f:
                self.log_database = pickle.load(f)
        except Exception:
            self.log_database = DefectLog()

    def get_all_defects_sorted(self):
        sorted_list = list(self.log_database.get_defects())
        sorted_list.sort()
        return sorted_list

    def count_defects_for_task(self, task_name):
        return sum(1 for d in self.log_database.get_defects() if d.task_name.lower() == task_name.lower())

    def get_admin_hash(self):
        return self.config.get_property("admin.hash")

# Streamlit caches the controller so it persists across page reloads
@st.cache_resource
def get_controller():
    return TrackerController()

controller = get_controller()

# ==========================================
# 7. WEB VIEW (STREAMLIT GUI)
# ==========================================
st.set_page_config(page_title="Six Sigma Terminal", layout="wide")

# Initialize Session State
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.role = ""
    st.session_state.name = ""

def login_screen():
    st.title("Lean Six Sigma Authentication")
    st.write("Please select your system role to continue.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Manager (Admin)")
        with st.form("manager_login"):
            admin_name = st.text_input("Name")
            admin_pass = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login as Manager")
            
            if submitted:
                if SecurityUtils.hash_password(admin_pass) == controller.get_admin_hash():
                    st.session_state.logged_in = True
                    st.session_state.role = "MANAGER"
                    st.session_state.name = admin_name or "Admin"
                    st.rerun()
                else:
                    st.error("Access Denied. Security violation logged.")
                    
    with col2:
        st.subheader("Floor Worker")
        with st.form("worker_login"):
            worker_id = st.text_input("Worker ID/Name")
            submitted = st.form_submit_button("Login as Worker")
            
            if submitted:
                st.session_state.logged_in = True
                st.session_state.role = "WORKER"
                st.session_state.name = worker_id or "Worker"
                st.rerun()

def main_dashboard():
    st.title(f"Six Sigma Terminal - {st.session_state.role}")
    st.write(f"Welcome, **{st.session_state.name}**")
    
    if st.button("Logout", key="logout"):
        st.session_state.logged_in = False
        st.rerun()

    st.divider()

    # Layout: Sidebar for actions, Main area for data
    st.sidebar.header("Operations Pipeline")
    
    # 1. Log Defect (Available to everyone)
    with st.sidebar.expander("Log New Defect", expanded=True):
        with st.form("log_defect"):
            d_id = st.text_input("Defect ID (e.g., D-001)")
            d_task = st.text_input("Task Name")
            d_desc = st.text_area("Description")
            d_type = st.selectbox("Severity", ["Minor", "Critical"])
            submit_defect = st.form_submit_button("Submit")
            
            if submit_defect:
                import re
                if not re.match(r"^D-\d{3}$", d_id):
                    st.error("Invalid ID format! Must be D-XXX.")
                else:
                    try:
                        new_defect = DefectFactory.create_defect(d_type, d_id, d_task, d_desc, st.session_state.name, "N/A")
                        controller.report_defect(new_defect)
                        st.success(f"Defect {d_id} logged successfully.")
                    except CriticalHaltException as e:
                        st.error(str(e))

    # Manager Only Tools
    if st.session_state.role == "MANAGER":
        # 2. Resolve Defect
        with st.sidebar.expander("Manual Resolve"):
            r_id = st.text_input("Defect ID to Resolve")
            if st.button("Resolve"):
                try:
                    controller.process_resolution(r_id, st.session_state.name, "Fixed by manual protocol.")
                    st.success(f"Defect {r_id} marked as resolved.")
                except Exception as e:
                    st.error(str(e))

        # 3. Smart Suggest Fix
        with st.sidebar.expander("✨ Smart Suggest Fix"):
            s_id = st.text_input("Defect ID for Analysis")
            if st.button("Analyze"):
                target = controller.log_database.find_defect(s_id)
                if not target:
                    st.error("Defect not found.")
                elif target.is_resolved:
                    st.info("Defect is already resolved.")
                else:
                    suggestion = ResolutionEngine.suggest_fix(target)
                    st.write("**SYSTEM ANALYSIS:**")
                    st.info(suggestion)
                    
                    # Need session state to hold the suggestion context to apply it
                    st.session_state.pending_smart_fix = (s_id, suggestion)

            if 'pending_smart_fix' in st.session_state:
                p_id, p_sugg = st.session_state.pending_smart_fix
                if st.button(f"Apply fix to {p_id}"):
                    try:
                        controller.process_resolution(p_id, st.session_state.name, f"Smart Fix: {p_sugg}")
                        st.success(f"Smart fix applied to {p_id}.")
                        del st.session_state.pending_smart_fix
                    except Exception as e:
                        st.error(str(e))

        # 4. Analytics
        with st.sidebar.expander("Sigma Analytics"):
            a_task = st.text_input("Task Name to Analyze")
            a_units = st.number_input("Total Units Produced", min_value=1, value=100)
            a_opps = st.number_input("Opportunities per Unit", min_value=1, value=5)
            
            if st.button("Generate Chart"):
                defects_count = controller.count_defects_for_task(a_task)
                total_opps = a_units * a_opps
                dpo = defects_count / total_opps if total_opps > 0 else 0
                dpmo = dpo * 1000000.0
                yield_val = (1.0 - dpo) * 100.0
                sigma = 6.0 if dpmo <= 3.4 else 5.0 if dpmo <= 233 else 4.0 if dpmo <= 6210 else 3.0
                
                st.sidebar.metric(label="Process Yield", value=f"{yield_val:.2f}%")
                st.sidebar.metric(label="Sigma Level", value=f"{sigma} σ")
                st.sidebar.text(f"DPO: {dpo:.6f}\nDPMO: {dpmo:.2f}")

        # 5. Database Management
        with st.sidebar.expander("Database Management"):
            if st.button("Clear Resolved Defects"):
                try:
                    controller.clear_resolved_defects()
                    st.success("Database cleared of resolved items.")
                except Exception as e:
                    st.warning(str(e))

    # --- Main Data Table ---
    search_term = st.text_input("🔍 Live Search Database:", "")
    
    defects = controller.get_all_defects_sorted()
    
    # Convert custom objects to dictionary for Pandas
    table_data = []
    for d in defects:
        row_text = f"{d.id} {d.task_name} {d.get_impact_level()} {d.logged_by} {d.description}".lower()
        if search_term.lower() in row_text:
            table_data.append({
                "ID": d.id,
                "Task": d.task_name,
                "Priority": d.get_impact_level(),
                "Status": "Resolved" if d.is_resolved else "Unresolved",
                "Aging": d.get_aging(),
                "Logged By": d.logged_by,
                "Description": d.description
            })
            
    df = pd.DataFrame(table_data)
    
    if not df.empty:
        # Define colors for the Priority column
        def highlight_critical(val):
            color = 'red' if val == 'CRITICAL' else 'black'
            return f'color: {color}'
        
        st.dataframe(df.style.map(highlight_critical, subset=['Priority']), use_container_width=True)
    else:
        st.info("No defects found matching your criteria.")

    # Export Button
    if st.session_state.role == "MANAGER":
        csv_data = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Pipeline Export (CSV)",
            data=csv_data,
            file_name='DataScience_Pipeline.csv',
            mime='text/csv',
        )

# Route the application based on session state
if not st.session_state.logged_in:
    login_screen()
else:
    main_dashboard()