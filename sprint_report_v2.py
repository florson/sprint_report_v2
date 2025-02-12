"""sprint report v2"""

import os
from datetime import datetime
from jira import JIRA
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Predefined list of boards to track
BOARDS = [
    2290, #'CIA Delivery',
    2299, #'Pricing&Promotions Delivery',
    2257, #'Product Delivery',
    2313, #'Direct Marketing Delivery Board',
    2258, #'PLP (Search) Delivery',
    2316, #'SPF Sprint',
    2319, #'BO Orders Sprint ITECOM',
    2388, #'Sinsay Club Delivery Board',
    #2613, #'FOX Delivery (sc)',
    2340, #'DEX Delivery',
    #2647, #'[DY] Dynamic Yield',
    #2684, #'CMS',
    2481, #'Product Platform',
    #2635, #'PaPay Scrum Team',
    #2634, #'Payments Scrum Team',
    2562 #'PCM 2.0'
]

FETCH_ALL_SPRINTS = False

def get_sprint_statistics(jira, board_name, board_id, existing_sprints=None):
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

                if not FETCH_ALL_SPRINTS and existing_sprints and f"{board_name}_{sprint.name}" in existing_sprints:
                    print(f"Pomijam istniejący sprint: {sprint.name}")
                    continue
                
                print(f"Przetwarzanie sprintu: {sprint.name}")
                # Skip incomplete or empty sprints
                if not getattr(sprint, 'completeDate', None):
                    print(f"Pomijam niezakończony sprint: {sprint.name}")
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

        if not FETCH_ALL_SPRINTS:
            existing_data = pd.read_csv('sprint_statistics.csv')
            existing_sprints = set(existing_data['board_name'] + '_' + existing_data['sprint_name'])
            print(f"Wczytano {len(existing_sprints)} istniejących sprintów")

        # Collect sprint statistics across all predefined boards
        all_sprint_stats = []
     
        for board_id in BOARDS:
            print(f"Przetwarzanie tablicy: {board_id}")
            boards = jira.boards(projectKeyOrID='ITECOM')
            board = next((board for board in boards if board.id == board_id), None)
            board_name = board.name

            if board is None:
                print(f"Nie znaleziono tablicy: {board_id}")
                continue

            print(f"Znaleziono tablicę: {board_name}")
            board_stats = get_sprint_statistics(jira, board_name, board.id, existing_sprints)
            all_sprint_stats.extend(board_stats)
            print(f"Przetworzono {len(board_stats)} sprintów dla tablicy {board_name}")

        
        if all_sprint_stats:
            # Convert new data to DataFrame
            new_data = pd.DataFrame(all_sprint_stats)
            
            # Combine with existing data if not fetching all
            if not FETCH_ALL_SPRINTS and not existing_data.empty:
                df = pd.concat([existing_data, new_data])
            else:
                df = new_data
            
            # Sort by date
            #df['end_date'] = pd.to_datetime(df['end_date'])
            #df = df.sort_values('end_date')

            # Optional: Save to CSV
            df.to_csv('sprint_statistics.csv', index=False)

            return df
        
        else:
            print("Brak nowych danych do zapisania")
            return existing_data

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
