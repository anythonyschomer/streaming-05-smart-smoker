bash
#!/bin/bash

# Path to the CSV file
CSV_FILE="path/to/your/csv/file.csv"

# Check if the file exists
if [ ! -f "$CSV_FILE" ]; then
    echo "Error: $CSV_FILE not found"
    exit 1
fi

# Set the field separator to comma
IFS=','

# Read the CSV file line by line
while read -r time channel1 channel2 channel3
do
    # Process each line as needed
    echo "Time: $time, Channel1: $channel1, Channel2: $channel2, Channel3: $channel3"
    
    # Add any additional processing logic here
done < "$CSV_FILE"