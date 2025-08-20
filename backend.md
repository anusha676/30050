# 30050
# backend_fin.py
# This file contains all the database interaction logic for the HR Employee Manager.
# It is designed to be a self-contained module for all CRUD operations and data insights.

import psycopg2
from psycopg2 import sql
import pandas as pd
from datetime import timedelta

# It is best practice to handle database credentials securely, e.g., using environment variables or a secrets management system.
# For this example, replace the placeholder values with your actual PostgreSQL connection details.
DB_PARAMS = {
    "dbname": "HR",
    "user": "Postgres",
    "password": "Anusha@369",
    "host": "localhost",
    "port": "5432"
}

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error connecting to database: {e}")
        return None

################################################################################
#                                                                              #
#                            CRUD OPERATIONS - EMPLOYEES                       #
#                                                                              #
################################################################################

def create_employee(first_name, last_name, contact_info, job_title, start_date, department_id):
    """Inserts a new employee record into the employees table."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO employees (first_name, last_name, contact_info, job_title, start_date, department_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (first_name, last_name, contact_info, job_title, start_date, department_id))
            conn.commit()
            return True, "Employee added successfully."
        except Exception as e:
            conn.rollback()
            return False, f"Error adding employee: {e}"
        finally:
            conn.close()
    return False, "Database connection failed."

def read_employees(search_query="", department_filter=None, job_title_filter=None, sort_by="start_date"):
    """
    Retrieves employee records with filtering and sorting options.
    Returns a pandas DataFrame.
    """
    conn = get_db_connection()
    if conn:
        try:
            query = """
                SELECT
                    e.employee_id,
                    e.first_name,
                    e.last_name,
                    e.contact_info,
                    e.job_title,
                    e.start_date,
                    d.department_name
                FROM
                    employees e
                LEFT JOIN
                    departments d ON e.department_id = d.department_id
                WHERE
                    (e.first_name ILIKE %s OR e.last_name ILIKE %s OR e.contact_info ILIKE %s)
            """
            params = (f'%{search_query}%', f'%{search_query}%', f'%{search_query}%')

            if department_filter:
                query += " AND d.department_name = %s"
                params += (department_filter,)
            
            if job_title_filter:
                query += " AND e.job_title = %s"
                params += (job_title_filter,)

            if sort_by == 'start_date':
                query += " ORDER BY e.start_date"
            elif sort_by == 'job_title':
                query += " ORDER BY e.job_title"
            elif sort_by == 'department':
                query += " ORDER BY d.department_name"

            df = pd.read_sql(query, conn, params=params)
            return True, df
        except Exception as e:
            return False, f"Error reading employees: {e}"
        finally:
            conn.close()
    return False, "Database connection failed."

def update_employee(employee_id, first_name, last_name, contact_info, job_title, start_date, department_id):
    """Updates an existing employee record."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE employees
                SET first_name = %s, last_name = %s, contact_info = %s, job_title = %s, start_date = %s, department_id = %s
                WHERE employee_id = %s
            """, (first_name, last_name, contact_info, job_title, start_date, department_id, employee_id))
            conn.commit()
            return True, "Employee updated successfully."
        except Exception as e:
            conn.rollback()
            return False, f"Error updating employee: {e}"
        finally:
            conn.close()
    return False, "Database connection failed."

def delete_employee(employee_id):
    """Deletes an employee record."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM employees WHERE employee_id = %s", (employee_id,))
            conn.commit()
            return True, "Employee deleted successfully."
        except Exception as e:
            conn.rollback()
            return False, f"Error deleting employee: {e}"
        finally:
            conn.close()
    return False, "Database connection failed."

################################################################################
#                                                                              #
#                          CRUD OPERATIONS - DEPARTMENTS                       #
#                                                                              #
################################################################################

def create_department(department_name):
    """Inserts a new department record."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO departments (department_name) VALUES (%s)", (department_name,))
            conn.commit()
            return True, "Department added successfully."
        except Exception as e:
            conn.rollback()
            return False, f"Error adding department: {e}"
        finally:
            conn.close()
    return False, "Database connection failed."

def read_departments():
    """Retrieves all departments."""
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql("SELECT * FROM departments", conn)
            return True, df
        except Exception as e:
            return False, f"Error reading departments: {e}"
        finally:
            conn.close()
    return False, "Database connection failed."

################################################################################
#                                                                              #
#                      CRUD OPERATIONS - PERFORMANCE REVIEWS                   #
#                                                                              #
################################################################################

def create_performance_review(employee_id, review_date, reviewer, comments, rating):
    """Inserts a new performance review record."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO performance_reviews (employee_id, review_date, reviewer, comments, rating)
                VALUES (%s, %s, %s, %s, %s)
            """, (employee_id, review_date, reviewer, comments, rating))
            conn.commit()
            return True, "Review added successfully."
        except Exception as e:
            conn.rollback()
            return False, f"Error adding review: {e}"
        finally:
            conn.close()
    return False, "Database connection failed."

def read_performance_reviews(sort_by="review_date"):
    """Retrieves all performance reviews with sorting options."""
    conn = get_db_connection()
    if conn:
        try:
            query = """
                SELECT
                    pr.review_id,
                    e.first_name,
                    e.last_name,
                    pr.review_date,
                    pr.reviewer,
                    pr.comments,
                    pr.rating
                FROM
                    performance_reviews pr
                JOIN
                    employees e ON pr.employee_id = e.employee_id
            """
            if sort_by == 'review_date':
                query += " ORDER BY pr.review_date DESC"
            elif sort_by == 'rating':
                query += " ORDER BY pr.rating DESC"
            
            df = pd.read_sql(query, conn)
            return True, df
        except Exception as e:
            return False, f"Error reading reviews: {e}"
        finally:
            conn.close()
    return False, "Database connection failed."

################################################################################
#                                                                              #
#                        CRUD OPERATIONS - LEAVE REQUESTS                      #
#                                                                              #
################################################################################

def create_leave_request(employee_id, leave_type, start_date, end_date):
    """Inserts a new leave request record."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO leave_requests (employee_id, leave_type, start_date, end_date)
                VALUES (%s, %s, %s, %s)
            """, (employee_id, leave_type, start_date, end_date))
            conn.commit()
            return True, "Leave request submitted successfully."
        except Exception as e:
            conn.rollback()
            return False, f"Error submitting leave request: {e}"
        finally:
            conn.close()
    return False, "Database connection failed."

def read_leave_requests():
    """Retrieves all leave requests."""
    conn = get_db_connection()
    if conn:
        try:
            query = """
                SELECT
                    lr.leave_id,
                    e.first_name,
                    e.last_name,
                    lr.leave_type,
                    lr.start_date,
                    lr.end_date,
                    lr.status
                FROM
                    leave_requests lr
                JOIN
                    employees e ON lr.employee_id = e.employee_id
            """
            df = pd.read_sql(query, conn)
            return True, df
        except Exception as e:
            return False, f"Error reading leave requests: {e}"
        finally:
            conn.close()
    return False, "Database connection failed."

def update_leave_status(leave_id, status):
    """Updates the status of a leave request."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("UPDATE leave_requests SET status = %s WHERE leave_id = %s", (status, leave_id))
            conn.commit()
            return True, "Leave status updated successfully."
        except Exception as e:
            conn.rollback()
            return False, f"Error updating leave status: {e}"
        finally:
            conn.close()
    return False, "Database connection failed."

def get_job_titles():
    """Retrieves a list of unique job titles."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT job_title FROM employees")
            titles = [row[0] for row in cur.fetchall()]
            return titles
        except Exception as e:
            print(f"Error fetching job titles: {e}")
            return []
        finally:
            conn.close()
    return []

################################################################################
#                                                                              #
#                            BUSINESS INSIGHTS                                 #
#                                                                              #
################################################################################

def get_total_employees():
    """Returns the total number of employees using COUNT."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM employees")
            count = cur.fetchone()[0]
            return count
        except Exception as e:
            print(f"Error getting total employees: {e}")
            return None
        finally:
            conn.close()
    return None

def get_total_departments():
    """Returns the total number of departments using COUNT."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM departments")
            count = cur.fetchone()[0]
            return count
        except Exception as e:
            print(f"Error getting total departments: {e}")
            return None
        finally:
            conn.close()
    return None

def get_avg_performance_rating():
    """Returns the average performance rating using AVG."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT AVG(rating) FROM performance_reviews")
            avg_rating = cur.fetchone()[0]
            return round(avg_rating, 2) if avg_rating else 0
        except Exception as e:
            print(f"Error getting average rating: {e}")
            return None
        finally:
            conn.close()
    return None

def get_leave_status_counts():
    """Returns the count of leave requests by status using COUNT and GROUP BY."""
    conn = get_db_connection()
    if conn:
        try:
            query = """
                SELECT status, COUNT(*) as count
                FROM leave_requests
                GROUP BY status
            """
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"Error getting leave status counts: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    return pd.DataFrame()

def get_employee_distribution_by_department():
    """Returns employee counts by department for charting."""
    conn = get_db_connection()
    if conn:
        try:
            query = """
                SELECT
                    d.department_name,
                    COUNT(e.employee_id) AS employee_count
                FROM
                    employees e
                LEFT JOIN
                    departments d ON e.department_id = d.department_id
                GROUP BY
                    d.department_name
                ORDER BY
                    employee_count DESC
            """
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"Error getting employee distribution: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    return pd.DataFrame()

def get_top_performing_departments():
    """Returns departments with their average review rating."""
    conn = get_db_connection()
    if conn:
        try:
            query = """
                SELECT
                    d.department_name,
                    AVG(pr.rating) AS avg_rating
                FROM
                    performance_reviews pr
                JOIN
                    employees e ON pr.employee_id = e.employee_id
                JOIN
                    departments d ON e.department_id = d.department_id
                GROUP BY
                    d.department_name
                ORDER BY
                    avg_rating DESC
            """
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"Error getting top-performing departments: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    return pd.DataFrame()

def get_leave_trends():
    """
    Returns the total number of leave days by month.
    NOTE: This calculation assumes `end_date - start_date` gives a time delta.
    """
    conn = get_db_connection()
    if conn:
        try:
            query = """
                SELECT
                    EXTRACT(YEAR FROM start_date) AS year,
                    EXTRACT(MONTH FROM start_date) AS month,
                    SUM(end_date - start_date) AS total_leave_days
                FROM
                    leave_requests
                GROUP BY
                    year, month
                ORDER BY
                    year, month
            """
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"Error getting leave trends: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    return pd.DataFrame()

def get_turnover_rate():
    """
    Placeholder function for turnover rate. The current schema for employees table does not
    include an `end_date` to track employee departures. This would require an schema update.
    The logic here is conceptual.
    """
    print("Cannot calculate turnover rate with the current schema. The 'employees' table needs an 'end_date' field.")
    return None
