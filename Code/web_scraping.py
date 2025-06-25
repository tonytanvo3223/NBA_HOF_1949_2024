from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import os
import csv
import re

# === SETTINGS ===
start_year = 2019  # 1951-1952 season
end_year = 2019    # 2023-2024 season
save_path = "nba_advanced_stats"  # Folder to save the CSVs

# Make sure folder exists
os.makedirs(save_path, exist_ok=True)

def setup_driver():
    """Start WebDriver with correct options."""
    try:
        # Try Safari first
        driver = webdriver.Safari()
    except Exception as e:
        print(f"Safari WebDriver failed: {e}")
        try:
            # Fall back to Chrome (more widely compatible)
            from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.chrome.options import Options
            from webdriver_manager.chrome import ChromeDriverManager
            
            options = Options()
            options.add_argument("--headless")  # Run in background
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        except Exception as e:
            print(f"Chrome WebDriver failed: {e}")
            raise Exception("Could not initialize any WebDriver. Please install Chrome or Safari WebDriver.")
    
    # Set default timeout
    driver.set_page_load_timeout(30)
    return driver

def scrape_table_directly(driver, url, season_str):
    """Directly scrape the table using its structure."""
    print(f"Processing: {url}")
    try:
        driver.get(url)
        # Wait for page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table#advanced_stats"))
        )
        
        # Find the advanced stats table
        table = driver.find_element(By.CSS_SELECTOR, "table#advanced_stats")
        
        # Extract headers
        headers = []
        header_row = table.find_element(By.CSS_SELECTOR, "thead tr:not(.over_header)")
        header_cells = header_row.find_elements(By.TAG_NAME, "th")
        for cell in header_cells:
            headers.append(cell.text.strip())
        
        # Remove empty header for the rank column
        if headers[0] == '':
            headers[0] = 'Rk'
            
        # Extract rows
        rows = []
        body_rows = table.find_elements(By.CSS_SELECTOR, "tbody tr:not(.thead)")
        for row in body_rows:
            # Skip blank/header rows
            if "thead" in row.get_attribute("class") or not row.is_displayed():
                continue
                
            row_data = []
            cells = row.find_elements(By.TAG_NAME, "td")
            cell_idx = 0
            
            # Process rank manually 
            rank_cell = row.find_element(By.TAG_NAME, "th")
            row_data.append(rank_cell.text.strip())
            
            # Process remaining cells
            for cell in cells:
                row_data.append(cell.text.strip())
                cell_idx += 1
                
            # Only add if it's an actual player row
            if len(row_data) > 5:  # Adjust this condition if needed
                rows.append(row_data)
        
        return headers, rows
    except TimeoutException:
        print(f"Timeout loading page: {url}")
        return None, None
    except NoSuchElementException as e:
        print(f"Table not found on {url}: {e}")
        return None, None
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None, None

def scrape_using_comment_content(driver, url, season_str):
    """Attempt to scrape data from commented HTML."""
    print(f"Attempting to extract from comments: {url}")
    try:
        driver.get(url)
        time.sleep(3)  # Wait for page to load fully
        
        # Get page source
        page_source = driver.page_source
        
        # Look for the commented table
        pattern = r'<!--\s*<div class="table_container" id="div_advanced_stats">(.*?)</table>\s*-->'
        match = re.search(pattern, page_source, re.DOTALL)
        
        if match:
            comment_html = match.group(1)
            
            # Save the comment to a temporary file
            temp_file = os.path.join(save_path, "_temp.html")
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write("<table>" + comment_html + "</table>")
            
            # Load the temp file
            driver.get("file://" + os.path.abspath(temp_file))
            
            # Now extract the data from the loaded table
            table = driver.find_element(By.TAG_NAME, "table")
            
            # Extract headers
            headers = []
            header_row = table.find_element(By.CSS_SELECTOR, "thead tr:not(.over_header)")
            header_cells = header_row.find_elements(By.TAG_NAME, "th")
            for cell in header_cells:
                headers.append(cell.text.strip())
            
            # Remove empty header for the rank column
            if headers[0] == '':
                headers[0] = 'Rk'
                
            # Extract rows
            rows = []
            body_rows = table.find_elements(By.CSS_SELECTOR, "tbody tr:not(.thead)")
            for row in body_rows:
                # Skip header rows
                if "thead" in row.get_attribute("class"):
                    continue
                    
                row_data = []
                cells = row.find_elements(By.TAG_NAME, "td")
                
                # Process rank manually if present
                try:
                    rank_cell = row.find_element(By.TAG_NAME, "th")
                    row_data.append(rank_cell.text.strip())
                except:
                    row_data.append("")  # No rank cell found
                
                # Process remaining cells
                for cell in cells:
                    row_data.append(cell.text.strip())
                
                if len(row_data) > 5:  # Only add if it's an actual player row
                    rows.append(row_data)
            
            # Clean up
            os.remove(temp_file)
            
            return headers, rows
        else:
            print(f"No commented table found in: {url}")
            return None, None
    except Exception as e:
        print(f"Error extracting from comments in {url}: {e}")
        return None, None

def alternative_scrape(driver, url, season_str):
    """Try to scrape in a different way for older seasons."""
    try:
        driver.get(url)
        time.sleep(3)  # Wait for page to load
        
        # Direct JavaScript execution to get the table
        script = """
        var table = document.querySelector('table#advanced_stats');
        if (!table) {
            // Look for any advanced stats table
            table = document.querySelector('table[id*="advanced"]');
        }
        if (!table) return null;
        
        var headers = [];
        var headerRow = table.querySelector('thead tr:not(.over_header)');
        if (!headerRow) headerRow = table.querySelector('thead tr');
        
        var headerCells = headerRow.querySelectorAll('th');
        for (var i = 0; i < headerCells.length; i++) {
            headers.push(headerCells[i].textContent.trim());
        }
        
        var rows = [];
        var bodyRows = table.querySelectorAll('tbody tr');
        for (var j = 0; j < bodyRows.length; j++) {
            var row = bodyRows[j];
            if (row.className.includes('thead')) continue;
            
            var rowData = [];
            var rankCell = row.querySelector('th');
            if (rankCell) rowData.push(rankCell.textContent.trim());
            
            var cells = row.querySelectorAll('td');
            for (var k = 0; k < cells.length; k++) {
                rowData.push(cells[k].textContent.trim());
            }
            
            if (rowData.length > 5) rows.push(rowData);
        }
        
        return {headers: headers, rows: rows};
        """
        
        result = driver.execute_script(script)
        
        if result:
            return result['headers'], result['rows']
        else:
            print(f"Alternative scraping failed for: {url}")
            return None, None
    except Exception as e:
        print(f"Error in alternative scraping for {url}: {e}")
        return None, None

def save_to_csv(headers, rows, filename):
    """Save the extracted data to a CSV file."""
    if not headers or not rows:
        return False
        
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        print(f"✅ Successfully saved: {filename}")
        return True
    except Exception as e:
        print(f"❌ Error saving CSV {filename}: {e}")
        return False

def main():
    driver = setup_driver()
    
    try:
        for year in range(start_year, end_year + 1):
            prev_year = year - 1
            season_str = f"{prev_year}_{year}"
            
            # === REGULAR SEASON ===
            reg_url = f"https://www.basketball-reference.com/leagues/NBA_{year}_advanced.html"
            reg_csv = os.path.join(save_path, f"rg_{season_str}.csv")
            
            # Try multiple methods until one works
            headers, rows = scrape_table_directly(driver, reg_url, season_str)
            
            if not headers or not rows:
                headers, rows = scrape_using_comment_content(driver, reg_url, season_str)
                
            if not headers or not rows:
                headers, rows = alternative_scrape(driver, reg_url, season_str)
            
            if save_to_csv(headers, rows, reg_csv):
                print(f"Regular season {season_str} processed successfully.")
            else:
                print(f"❌ Failed to save regular season data for {season_str}")
            
            # === PLAYOFFS ===
            po_url = f"https://www.basketball-reference.com/playoffs/NBA_{year}_advanced.html"
            po_csv = os.path.join(save_path, f"po_{season_str}.csv")
            
            # Try multiple methods until one works
            headers, rows = scrape_table_directly(driver, po_url, season_str)
            
            if not headers or not rows:
                headers, rows = scrape_using_comment_content(driver, po_url, season_str)
                
            if not headers or not rows:
                headers, rows = alternative_scrape(driver, po_url, season_str)
            
            if save_to_csv(headers, rows, po_csv):
                print(f"Playoffs {season_str} processed successfully.")
            else:
                print(f"❌ Failed to save playoff data for {season_str}")
            
            # Be nice to the server
            time.sleep(2)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()