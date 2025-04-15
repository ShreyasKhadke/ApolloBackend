"""
Combinations Generator - Handles the generation and tracking of Apollo search combinations
"""
import time
import json
import csv
import pymongo
from itertools import product
from datetime import datetime
from django.conf import settings
from django.utils import timezone


def get_completed_combinations():
    """
    Query MongoDB to find combinations that have been completed
    Returns a set of tuples (location, industry_name) that have been completed
    """
    combinations_collection = settings.MONGO_DB['combinations']
    completed_combinations = set()
    
    # Find all completed combinations
    cursor = combinations_collection.find({"status": "completed"}, {"location": 1, "industry_name": 1})
    
    for combo in cursor:
        completed_combinations.add((combo["location"], combo["industry_name"]))
    
    return completed_combinations

def generate_combinations():
    """
    Generate all combinations of locations, employee ranges, and industries.
    Store them in MongoDB if they don't already exist.
    Returns a list of dictionaries with location, employee_ranges, industry_id, and industry_name.
    """
    # Start timer for the whole function
    start_time = time.time()
    print(f"Starting combination generation at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    combinations_collection = settings.MONGO_DB['combinations']
    
    # Read locations from CSV
    locations_start = time.time()
    print("Reading locations from CSV...")
    locations = []
    with open('data/all_Cities.csv', 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            locations.append(row['location'])
    locations_end = time.time()
    print(f"Read {len(locations)} locations in {locations_end - locations_start:.2f} seconds")
    
    # Define the employee ranges
    employee_ranges = ["1-10, 10-20, 20-50, 50-100, 100-200"]
    
    # Read industry tags from the formatted JSON file
    industry_start = time.time()
    print("Reading industry tags...")
    with open('data/formatted_industry_tags.json', 'r', encoding='utf-8') as jsonfile:
        industry_tags = json.load(jsonfile)
    
    # Extract just the industry names (keys from the dictionary)
    apollo_industries = list(industry_tags.keys())
    industry_end = time.time()
    print(f"Read {len(apollo_industries)} industries in {industry_end - industry_start:.2f} seconds")
    
    # Calculate total potential combinations
    total_potential = len(locations) * len(employee_ranges) * len(apollo_industries)
    print(f"Potential combinations to process: {total_potential}")
    
    # Check if all combinations already exist in the database
    existing_count = combinations_collection.count_documents({})
    
    # If the existing count matches the potential total, we can skip the generation step
    if existing_count == total_potential:
        print(f"All {total_potential} combinations already exist in the database. Skipping storage step.")
        
        # Count combinations by status for reporting
        pending_count = combinations_collection.count_documents({"status": "pending"})
        completed_count = combinations_collection.count_documents({"status": "completed"})
        failed_count = combinations_collection.count_documents({"status": "failed"})
        in_progress_count = combinations_collection.count_documents({"status": "in_progress"})
        
        return {
            "total": total_potential,
            "pending": pending_count,
            "completed": completed_count,
            "failed": failed_count,
            "in_progress": in_progress_count,
            "skipped_generation": True
        }
    
    # Count of new combinations added
    new_combinations_count = 0
    processed_count = 0
    batch_size = 100000  # Increased batch size to 100,000 for maximum performance
    current_batch = []
    
    # Generate combinations and store in MongoDB
    combination_start = time.time()
    print("Generating and storing combinations...")
    
    try:
        for location, size, industry in product(locations, employee_ranges, apollo_industries):
            # Prepare the combination
            industry_id = industry_tags[industry]  # Get the ID for the industry
            industry_name = industry
            
            # Create a dictionary for this combination
            combination = {
                "location": location,
                "employee_ranges": size,
                "industry_id": industry_id,
                "industry_name": industry_name,
                "status": "pending",  # Initial status
                "created_at": timezone.now(),
                "updated_at": timezone.now()
            }
            
            # Add to current batch
            current_batch.append({
                "filter": {"location": location, "industry_name": industry_name},
                "update": {"$setOnInsert": combination},
                "upsert": True
            })
            
            processed_count += 1
            
            # Process batch if it reaches the batch size
            if len(current_batch) >= batch_size:
                try:
                    result = combinations_collection.bulk_write(
                        [pymongo.UpdateOne(**doc) for doc in current_batch],
                        ordered=False  # Use unordered execution for better performance
                    )
                    new_combinations_count += result.upserted_count
                    
                    # Log progress every 5 batches
                    if (processed_count // batch_size) % 5 == 0:
                        progress_pct = (processed_count / total_potential) * 100
                        elapsed = time.time() - combination_start
                        estimated_total = (total_potential / processed_count) * elapsed if processed_count > 0 else 0
                        remaining = estimated_total - elapsed
                        
                        # Format time remaining
                        mins_remaining = int(remaining // 60)
                        secs_remaining = int(remaining % 60)
                        
                        print(f"Progress: {processed_count}/{total_potential} ({progress_pct:.2f}%) - Added {new_combinations_count} new combinations")
                        print(f"Estimated time remaining: {mins_remaining} minutes {secs_remaining} seconds")
                    
                    # Clear batch
                    current_batch = []
                except Exception as e:
                    print(f"Error inserting batch: {str(e)}")
                    # Continue with an empty batch
                    current_batch = []
        
        # Process remaining items in the last batch
        if current_batch:
            try:
                result = combinations_collection.bulk_write(
                    [pymongo.UpdateOne(**doc) for doc in current_batch], 
                    ordered=False
                )
                new_combinations_count += result.upserted_count
            except Exception as e:
                print(f"Error inserting final batch: {str(e)}")
    except Exception as e:
        print(f"Error during combination generation: {str(e)}")
    
    combination_end = time.time()
    print(f"Generated and stored combinations in {combination_end - combination_start:.2f} seconds")
    print(f"Added {new_combinations_count} new combinations to MongoDB")
    
    # Now retrieve all combinations from MongoDB to return - more efficiently
    retrieve_start = time.time()
    print("Retrieving combinations count from MongoDB...")
    
    # Count combinations more efficiently by status
    total_combinations = combinations_collection.count_documents({})
    pending_count = combinations_collection.count_documents({"status": "pending"})
    completed_count = combinations_collection.count_documents({"status": "completed"})
    failed_count = combinations_collection.count_documents({"status": "failed"})
    in_progress_count = combinations_collection.count_documents({"status": "in_progress"})
    
    print(f"Total combinations in database: {total_combinations}")
    print(f"  Pending: {pending_count}")
    print(f"  Completed: {completed_count}")
    print(f"  Failed: {failed_count}")
    print(f"  In Progress: {in_progress_count}")
    
    retrieve_end = time.time()
    print(f"Retrieved combination counts in {retrieve_end - retrieve_start:.2f} seconds")
    
    # Calculate total time
    total_time = time.time() - start_time
    minutes, seconds = divmod(total_time, 60)
    hours, minutes = divmod(minutes, 60)
    
    print(f"Total combination generation time: {int(hours)}h {int(minutes)}m {seconds:.2f}s")
    print(f"Combination generation completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Fetching combinations with query: {{'status': {{'$in': ['pending', 'failed']}}}}")
    
    # Return stats instead of combinations list to save memory
    return {
        "total": total_combinations,
        "pending": pending_count,
        "completed": completed_count,
        "failed": failed_count,
        "in_progress": in_progress_count,
        "new_added": new_combinations_count,
        "skipped_generation": False
    }

def update_combination_status(location, industry_name, status, results_count=None):
    """
    Update the status of a combination in MongoDB
    """
    combinations_collection = settings.MONGO_DB['combinations']
    
    update_data = {
        "status": status,
        "updated_at": timezone.now()
    }
    
    # Add results count if provided
    if results_count is not None:
        update_data["results_count"] = results_count
    
    # Update the combination
    combinations_collection.update_one(
        {"location": location, "industry_name": industry_name},
        {"$set": update_data}
    )
    
    print(f"Updated combination status: {location} - {industry_name} => {status}")