"""sprint report v2"""

import os
from datetime import datetime
from jira import JIRA
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Predefined list of boards to track
BOARDS = [
    'CIA Delivery',
    'Pricing&Promotions Delivery',
    'Product Delivery',
    'Direct Marketing Delivery Board',
    'PLP (Search) Delivery',
    'SPF Sprint',
    'BO Orders Sprint ITECOM',
    'Sinsay Club Delivery Board',
    'FOX Delivery (sc)',
    'DEX Delivery'
]

def get_sprint_statistics(jira, board_name, board_id):
    """
    Retrieve and calculate sprint statistics for a specific board.

    Args:
        jira: JIRA client instance
        board_name (str): Name of the board
        board_id (int): ID of the board

    Returns:
        list: List of dictionaries containing sprint statistics
    """
    board_sprints = []
    start_at = 0
    max_results = 50

    print(f"Pobieranie sprintów dla tablicy {board_name}")

    while True:
        sprints = jira.sprints(board_id, startAt=start_at, maxResults=max_results)
        print(f"Pobrano {len(sprints)} sprintów")

        for sprint in sprints:
            try:
                print(f"Przetwarzanie sprintu: {sprint.name}")
                # Skip incomplete or empty sprints
                if not sprint.startDate or not sprint.completeDate:
                    continue

                sprint_id = sprint.id

                start_date = datetime.fromisoformat(sprint.startDate)
                end_date = datetime.fromisoformat(sprint.completeDate)

                start_date_str = start_date.strftime('%Y-%m-%d %H:%M')
                end_date_str = end_date.strftime('%Y-%m-%d %H:%M')

                # JQL queries
                completed_issues_jql = f'sprint = {sprint_id} AND resolution changed TO "done" DURING  ("{start_date_str}", "{end_date_str}")'
                added_after_start_jql = f'sprint = {sprint_id} AND issueFunction in addedAfterSprintStart("{board_name}", "{sprint.name}")'
                removed_after_start_jql = f'issueFunction in removedAfterSprintStart("{board_name}", "{sprint.name}")'
                planned_issues_jql = f'sprint = {sprint_id}'
                bugs_jql = f'sprint = {sprint_id} and issuetype = bug'

                # Searching issues
                completed_issues = jira.search_issues(completed_issues_jql, maxResults=False)
                added_after_start_issues = jira.search_issues(added_after_start_jql, maxResults=False)
                planned_issues = jira.search_issues(planned_issues_jql, maxResults=False)
                removed_issues = jira.search_issues(removed_after_start_jql, maxResults=False)
                bugs = jira.search_issues(bugs_jql, maxResults=False)

                # Calculating statistics
                burned_total = sum(issue.fields.customfield_10019 for issue in completed_issues if issue.fields.customfield_10019)
                burned = sum(issue.fields.customfield_10019 for issue in completed_issues if issue.fields.customfield_10019 and issue not in added_after_start_issues)
                removed_after_sprint_start = sum(issue.fields.customfield_10019 for issue in removed_issues if issue.fields.customfield_10019)
                added_after_sprint_start = sum(issue.fields.customfield_10019 for issue in added_after_start_issues if issue.fields.customfield_10019)
                plan_total = sum(issue.fields.customfield_10019 for issue in planned_issues if issue.fields.customfield_10019)
                plan = sum(issue.fields.customfield_10019 for issue in planned_issues if issue.fields.customfield_10019 and issue not in added_after_start_issues)
                bugs_count = sum(1 for issue in bugs)

                predictability = round(burned / plan * 100, 0) if plan > 0 else 0

                board_sprints.append({
                    'board_name': board_name,
                    'sprint_name': sprint.name,
                    'start_date': start_date_str,
                    'end_date': end_date_str,
                    'original_plan_sp': plan,
                    'total_plan_sp': plan_total,
                    'burned_from_plan_sp': burned,
                    'total_burned_sp': burned_total,
                    'predictability_percentage': predictability,
                    'bugs_count': bugs_count,
                    'added_during_sprint_sp': added_after_sprint_start,
                    'removed_during_sprint_sp': removed_after_sprint_start
                })

            except Exception as e:
                print(f"Error processing sprint {sprint.name} on board {board_name}: {str(e)}")

        if len(sprints) < max_results:
            break
        start_at += max_results

    print(f"Zakończono pobieranie sprintów dla tablicy {board_name}")
    return board_sprints

def get_all_board_sprint_statistics():
    """
    Collect sprint statistics for all predefined boards and export to CSV.

    Returns:
        DataFrame: Pandas DataFrame containing sprint statistics, None if error occurs
    """
    try:
        # JIRA connection details
        jira_url = os.getenv('JIRA_URL')
        jira_username = os.getenv('JIRA_USERNAME')
        jira_password = os.getenv('JIRA_PASSWORD')

        print("Próba połączenia z JIRA...")
        jira = JIRA(server=jira_url, basic_auth=(jira_username, jira_password))
        print("Połączenie z JIRA udane.")

        # Collect sprint statistics across all predefined boards
        all_sprint_stats = []
        for board_name in BOARDS:
            print(f"Przetwarzanie tablicy: {board_name}")
            boards = jira.boards(projectKeyOrID='ITECOM')
            board = next((board for board in boards if board.name == board_name), None)

            if board is None:
                print(f"Nie znaleziono tablicy: {board_name}")
                continue

            print(f"Znaleziono tablicę o ID: {board.id}")
            board_stats = get_sprint_statistics(jira, board_name, board.id)
            all_sprint_stats.extend(board_stats)
            print(f"Przetworzono {len(board_stats)} sprintów dla tablicy {board_name}")

        # Convert to DataFrame
        df = pd.DataFrame(all_sprint_stats)

        # Optional: Save to CSV
        df.to_csv('sprint_statistics.csv', index=False)

        return df

    except Exception as e:
        print(f"Szczegółowy błąd: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Execute the sprint statistics collection and display results."""
    sprint_statistics_df = get_all_board_sprint_statistics()

    if sprint_statistics_df is not None:
        print(sprint_statistics_df)
        print("\nStatistics saved to sprint_statistics.csv")

if __name__ == "__main__":
    main()
