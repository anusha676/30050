# frontend_fin.py
# This file contains the Streamlit user interface for the HR Employee Manager.
# It interacts with the backend_fin module to perform all data operations.

import streamlit as st
import pandas as pd
from datetime import date
from backend import (
    create_employee, read_employees, update_employee, delete_employee,
    create_department, read_departments,
    create_performance_review, read_performance_reviews,
    create_leave_request, read_leave_requests, update_leave_status,
    get_total_employees, get_total_departments, get_avg_performance_rating,
    get_leave_status_counts, get_employee_distribution_by_department,
    get_top_performing_departments, get_leave_trends, get_job_titles
)

# Set the page title and layout
st.set_page_config(page_title="HR Employee Manager", layout="wide")

# --- Sidebar Navigation ---
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Go to",
    ("Dashboard", "Employee Management", "Department Management", "Performance Reviews", "Leave Requests", "Business Insights")
)

# --- Data Loading Utility ---
@st.cache_data
def load_employees_data():
    """Cached function to load employee data to prevent re-fetching on every interaction."""
    success, data = read_employees()
    return data

@st.cache_data
def load_departments_data():
    """Cached function to load department data."""
    success, data = read_departments()
    return data

def show_message(success, message):
    """Displays a success or error message."""
    if success:
        st.success(message)
    else:
        st.error(message)

# --- Main Page Content ---

if page == "Dashboard":
    st.title("📊 HR Dashboard")
    st.markdown("A quick overview of key HR metrics.")
    st.markdown("---")
    
    col1, col2, col3, col4 = st.columns(4)

    # Aggregation Metrics (st.metric)
    with col1:
        total_employees = get_total_employees()
        st.metric(label="Total Employees", value=total_employees or 0)

    with col2:
        total_departments = get_total_departments()
        st.metric(label="Total Departments", value=total_departments or 0)

    with col3:
        avg_rating = get_avg_performance_rating()
        st.metric(label="Avg. Performance Rating", value=avg_rating or "N/A", delta_color="normal")

    with col4:
        leave_counts_df = get_leave_status_counts()
        total_requests = leave_counts_df['count'].sum() if not leave_counts_df.empty else 0
        st.metric(label="Total Leave Requests", value=total_requests)

    if not leave_counts_df.empty:
        st.subheader("Leave Request Status Breakdown")
        st.bar_chart(leave_counts_df.set_index('status'))

if page == "Employee Management":
    st.title("👤 Employee Management")
    st.markdown("Add, view, and manage employee records.")
    
    st.subheader("Add New Employee")
    departments_df = load_departments_data()
    if not departments_df.empty:
        department_names = departments_df['department_name'].tolist()
        with st.form("add_employee_form"):
            col1, col2 = st.columns(2)
            with col1:
                first_name = st.text_input("First Name", help="Required")
                contact_info = st.text_input("Contact Info")
                start_date = st.date_input("Start Date", date.today(), help="Required")
            with col2:
                last_name = st.text_input("Last Name", help="Required")
                job_title = st.text_input("Job Title")
                selected_dept_name = st.selectbox("Department", department_names)
            
            submitted = st.form_submit_button("Add Employee")
            if submitted and first_name and last_name:
                dept_id = departments_df.loc[departments_df['department_name'] == selected_dept_name, 'department_id'].iloc[0]
                success, message = create_employee(first_name, last_name, contact_info, job_title, start_date, dept_id)
                show_message(success, message)
    else:
        st.warning("Please add departments first in the 'Department Management' section.")

    st.markdown("---")
    st.subheader("View and Filter Employees")
    
    # Filtering and Sorting
    col1, col2, col3 = st.columns(3)
    with col1:
        search_query = st.text_input("Search by Name or Contact Info")
    with col2:
        department_filter = st.selectbox("Filter by Department", ["All"] + department_names)
        department_filter = None if department_filter == "All" else department_filter
    with col3:
        job_titles = get_job_titles()
        job_title_filter = st.selectbox("Filter by Job Title", ["All"] + job_titles)
        job_title_filter = None if job_title_filter == "All" else job_title_filter
    
    sort_by = st.selectbox("Sort by", ["Start Date (Seniority)", "Job Title", "Department"])
    sort_by_map = {
        "Start Date (Seniority)": "start_date",
        "Job Title": "job_title",
        "Department": "department"
    }
    
    success, employees_df = read_employees(search_query, department_filter, job_title_filter, sort_by_map[sort_by])
    if success:
        st.dataframe(employees_df, use_container_width=True)
    else:
        st.error(employees_df)

if page == "Department Management":
    st.title("🏢 Department Management")
    st.markdown("Add new departments.")

    st.subheader("Add New Department")
    with st.form("add_department_form"):
        department_name = st.text_input("Department Name", help="Required")
        submitted = st.form_submit_button("Add Department")
        if submitted and department_name:
            success, message = create_department(department_name)
            show_message(success, message)
    
    st.markdown("---")
    st.subheader("Current Departments")
    success, departments_df = read_departments()
    if success:
        st.dataframe(departments_df, use_container_width=True)
    else:
        st.error(departments_df)

if page == "Performance Reviews":
    st.title("✍️ Performance Reviews")
    st.markdown("Add and view employee performance reviews.")
    
    st.subheader("Add New Review")
    employees_df = load_employees_data()
    if not employees_df.empty:
        employee_names = employees_df.apply(lambda row: f"{row['first_name']} {row['last_name']} (ID: {row['employee_id']})", axis=1).tolist()
        
        with st.form("add_review_form"):
            selected_employee_name = st.selectbox("Select Employee", employee_names)
            employee_id = int(selected_employee_name.split('(ID: ')[1][:-1])
            review_date = st.date_input("Review Date", date.today())
            reviewer = st.text_input("Reviewer Name")
            comments = st.text_area("Comments")
            rating = st.slider("Rating (1-5)", 1, 5, 3)
            submitted = st.form_submit_button("Add Review")
            
            if submitted:
                success, message = create_performance_review(employee_id, review_date, reviewer, comments, rating)
                show_message(success, message)
    else:
        st.warning("Please add employees first in the 'Employee Management' section.")

    st.markdown("---")
    st.subheader("View Performance Reviews")
    sort_by = st.selectbox("Sort by", ["Review Date", "Rating"])
    sort_by_map = {"Review Date": "review_date", "Rating": "rating"}
    
    success, reviews_df = read_performance_reviews(sort_by_map[sort_by])
    if success:
        st.dataframe(reviews_df, use_container_width=True)
    else:
        st.error(reviews_df)
    
if page == "Leave Requests":
    st.title("🌴 Leave Requests")
    st.markdown("Submit and approve/reject leave requests.")
    
    st.subheader("Submit New Leave Request")
    employees_df = load_employees_data()
    if not employees_df.empty:
        employee_names = employees_df.apply(lambda row: f"{row['first_name']} {row['last_name']} (ID: {row['employee_id']})", axis=1).tolist()
        
        with st.form("add_leave_form"):
            selected_employee_name = st.selectbox("Select Employee", employee_names)
            employee_id = int(selected_employee_name.split('(ID: ')[1][:-1])
            leave_type = st.selectbox("Leave Type", ["Sick", "Vacation", "Maternity", "Paternity"])
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start Date", date.today())
            with col2:
                end_date = st.date_input("End Date", date.today())
            
            submitted = st.form_submit_button("Submit Request")
            if submitted:
                success, message = create_leave_request(employee_id, leave_type, start_date, end_date)
                show_message(success, message)
    else:
        st.warning("Please add employees first in the 'Employee Management' section.")
        
    st.markdown("---")
    st.subheader("View and Manage Leave Requests")
    success, leave_df = read_leave_requests()
    if success and not leave_df.empty:
        st.dataframe(leave_df, use_container_width=True)
        
        st.subheader("Change Request Status")
        with st.form("update_leave_status_form"):
            leave_id = st.number_input("Enter Leave ID to Update", min_value=1, step=1)
            new_status = st.selectbox("Select New Status", ["Pending", "Approved", "Rejected"])
            submitted = st.form_submit_button("Update Status")
            if submitted:
                success, message = update_leave_status(leave_id, new_status)
                show_message(success, message)
    elif not success:
        st.error(leave_df)
    else:
        st.info("No leave requests found.")

if page == "Business Insights":
    st.title("📈 Business Insights")
    st.markdown("Dive deeper into workforce data with visualizations.")
    
    st.markdown("---")
    st.subheader("Employee Distribution by Department")
    emp_dist_df = get_employee_distribution_by_department()
    if not emp_dist_df.empty:
        st.bar_chart(emp_dist_df.set_index('department_name'))
    else:
        st.info("No data available for this chart. Please add employees and departments.")
        
    st.markdown("---")
    st.subheader("Top Performing Departments")
    top_performers_df = get_top_performing_departments()
    if not top_performers_df.empty:
        st.dataframe(top_performers_df, use_container_width=True)
        st.info("This table shows departments with the highest average review ratings.")
    else:
        st.info("No data available. Please add performance reviews.")
    
    st.markdown("---")
    st.subheader("Leave Trends")
    leave_trends_df = get_leave_trends()
    if not leave_trends_df.empty:
        st.bar_chart(leave_trends_df.set_index('year').set_index('month'))
    else:
        st.info("No data available for leave trends. Please submit leave requests.")
    
    # Placeholder for Turnover Rate, as schema is missing `end_date`
    st.markdown("---")
    st.subheader("Employee Turnover Rate")
    st.info("This insight requires an `end_date` column in the `employees` table to track when employees leave the company. This feature is not available with the current database schema.")
