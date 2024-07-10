import pandas as pd
import subprocess
from concurrent.futures import ThreadPoolExecutor

# Load the CSV file
file_path = './brokenlinks.csv'
df = pd.read_csv(file_path)

# Function to check the status of a URL using curl
def check_url_status(url):
    try:
        result = subprocess.run(['curl', '-I', '--insecure', '--max-time', '5', url], capture_output=True, text=True)
        if 'HTTP/' in result.stdout:
            status_line = result.stdout.split('\n')[0]
            status_code = int(status_line.split()[1])
            return url, status_code
        else:
            return url, 'Failed'
    except Exception as e:
        print(f"Request failed for {url}: {e}")
        return url, str(e)

# Function to process a batch of URLs
def process_batch(batch):
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(check_url_status, url): url for url in batch}
        for future in future_to_url:
            try:
                url, status = future.result()
                print(f"Processed {url}: {status}")
                results.append((url, status))
            except Exception as e:
                print(f"Error processing {future_to_url[future]}: {e}")
                results.append((future_to_url[future], str(e)))
    return results

# Split the URLs into smaller batches
batch_size = 20
batches = [df['URL'][i:i + batch_size] for i in range(0, len(df), batch_size)]

# Process each batch and collect results
all_results = []
for batch in batches:
    all_results.extend(process_batch(batch))

# Create a DataFrame with the results
results_df = pd.DataFrame(all_results, columns=['URL', 'Status'])

# Filter the DataFrame for URLs with status 404
broken_links_df = results_df[results_df['Status'] == 404]

# Save the filtered DataFrame to a new CSV file
output_path = './404.csv'
broken_links_df.to_csv(output_path, index=False)

# Output the path of the generated file
print(f"Filtered results saved to: {output_path}")
