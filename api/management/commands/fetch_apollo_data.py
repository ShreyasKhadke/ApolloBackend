"""
Django management command to run the Apollo Fetcher with MongoDB-based combination tracking
"""
import os
import sys
import time
import random
from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import timezone
from datetime import datetime
from utils.apollo_fetcher import (
    generate_combinations,
    search_apollo_and_save_to_mongodb,
    update_combination_status
)

class Command(BaseCommand):
    help = 'Fetch data from Apollo API and save to MongoDB'

    def add_arguments(self, parser):
        # Optional arguments to filter or customize the fetching process
        parser.add_argument('--location', type=str, help='Specific location to search for (optional)')
        parser.add_argument('--industry', type=str, help='Specific industry name to search for (optional)')
        parser.add_argument('--limit', type=int, help='Limit number of combinations to process (optional)')
        parser.add_argument('--cookies-file', type=str, default='data/cookies.json', help='Path to cookies JSON file')
        parser.add_argument('--headers-file', type=str, default='data/headers.json', help='Path to headers JSON file')
        parser.add_argument('--yes', action='store_true', help='Skip confirmation prompt')
        parser.add_argument('--reset-failed', action='store_true', help='Reset failed combinations to pending')
        parser.add_argument('--status', type=str, choices=['pending', 'in_progress', 'completed', 'failed'], 
                           help='Filter combinations by status')
        parser.add_argument('--skip-generation', action='store_true', 
                           help='Skip the combination generation step (use if they already exist)')
        parser.add_argument('--batch-size', type=int, default=5000,
                           help='Batch size for MongoDB operations during generation')

    def handle(self, *args, **options):
        # Force stdout to be unbuffered
        sys.stdout.reconfigure(line_buffering=True)
        self.stdout.write(self.style.SUCCESS("Output unbuffered - all logs will be displayed immediately"))
        
        # Check that the cookies and headers files exist
        cookies_file = options['cookies_file']
        headers_file = options['headers_file']
        
        if not os.path.exists(cookies_file):
            self.stdout.write(self.style.ERROR(f'Cookies file not found: {cookies_file}'))
            return
            
        if not os.path.exists(headers_file):
            self.stdout.write(self.style.ERROR(f'Headers file not found: {headers_file}'))
            return
        
        # Reset failed combinations if requested
        if options.get('reset_failed'):
            combinations_collection = settings.MONGO_DB['combinations']
            result = combinations_collection.update_many(
                {"status": "failed"},
                {"$set": {"status": "pending", "updated_at": timezone.now()}}
            )
            self.stdout.write(self.style.SUCCESS(f"Reset {result.modified_count} failed combinations to pending status"))
        
        # Generate combinations only if not skipped
        if not options.get('skip_generation'):
            # Generate all combinations (this will insert them into MongoDB if they don't exist)
            self.stdout.write("Generating and checking combinations in MongoDB...")
            self.stdout.write(self.style.WARNING("This may take a while for large datasets..."))
            all_combinations = generate_combinations()
        else:
            self.stdout.write(self.style.SUCCESS("Skipping combination generation step as requested"))
            # Just get a count of combinations
            combinations_collection = settings.MONGO_DB['combinations']
            count = combinations_collection.count_documents({})
            self.stdout.write(f"Found {count} existing combinations in MongoDB")
        
        # Get combinations collection for filtering
        combinations_collection = settings.MONGO_DB['combinations']
        
        # Build query for filtering combinations
        query = {}
        
        # Filter by status if specified
        status_filter = options.get('status')
        if status_filter:
            query["status"] = status_filter
        else:
            # Default to pending and failed combinations
            query["status"] = {"$in": ["pending", "failed"]}
        
        # Apply location filter if specified
        location_filter = options.get('location')
        if location_filter:
            query["location"] = {"$regex": location_filter, "$options": "i"}
        
        # Apply industry filter if specified
        industry_filter = options.get('industry')
        if industry_filter:
            query["industry_name"] = {"$regex": industry_filter, "$options": "i"}
        
        # Fetch filtered combinations from MongoDB - only needed fields
        self.stdout.write(f"Fetching combinations with query: {query}")
        cursor = combinations_collection.find(
            query, 
            {"location": 1, "employee_ranges": 1, "industry_id": 1, "industry_name": 1}
        )
        filtered_combinations = list(cursor)
        
        # Apply limit if specified
        limit = options.get('limit')
        if limit and limit > 0 and limit < len(filtered_combinations):
            self.stdout.write(f"Limiting to first {limit} combinations")
            filtered_combinations = filtered_combinations[:limit]
        
        # Count combinations by status
        total_combinations = combinations_collection.count_documents({})
        pending_count = len(filtered_combinations)
        completed_count = combinations_collection.count_documents({"status": "completed"})
        failed_count = combinations_collection.count_documents({"status": "failed"})
        in_progress_count = combinations_collection.count_documents({"status": "in_progress"})
        
        self.stdout.write(f"\nTotal combinations: {total_combinations}")
        self.stdout.write(f"Status breakdown:")
        self.stdout.write(f"  Completed: {completed_count}")
        self.stdout.write(f"  Pending: {combinations_collection.count_documents({'status': 'pending'})}")
        self.stdout.write(f"  Failed: {failed_count}")
        self.stdout.write(f"  In Progress: {in_progress_count}")
        self.stdout.write(f"Filtered combinations to process: {pending_count}")
        
        if pending_count == 0:
            self.stdout.write("No combinations matching the criteria. Nothing to do.")
            return
        
        
        # Stats for tracking progress
        start_time = time.time()
        completed_in_run = 0
        
        # Process each filtered combination
        for i, combo in enumerate(filtered_combinations):
            location = combo["location"]
            employee_ranges = combo["employee_ranges"]
            industry_id = combo["industry_id"]
            industry_name = combo["industry_name"]
            
            # Calculate progress
            processed = i + 1
            
            # Display progress information with timing stats
            if processed > 1:
                elapsed_time = time.time() - start_time
                avg_time_per_combo = elapsed_time / completed_in_run if completed_in_run > 0 else 0
                estimated_remaining = avg_time_per_combo * (pending_count - processed)
                
                # Format time remaining
                hours, remainder = divmod(estimated_remaining, 3600)
                minutes, seconds = divmod(remainder, 60)
                
                time_remaining = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
                
                self.stdout.write(f"\n{'='*50}")
                self.stdout.write(f"Processing combination {processed}/{pending_count}")
                self.stdout.write(f"Location: {location}")
                self.stdout.write(f"Industry: {industry_name}")
                self.stdout.write(f"Estimated time remaining: {time_remaining}")
                self.stdout.write(f"{'='*50}\n")
            else:
                self.stdout.write(f"\n{'='*50}")
                self.stdout.write(f"Processing combination {processed}/{pending_count}")
                self.stdout.write(f"Location: {location}")
                self.stdout.write(f"Industry: {industry_name}")
                self.stdout.write(f"{'='*50}\n")
            
            try:
                # Run search for this combination and save to MongoDB
                results_count = search_apollo_and_save_to_mongodb(
                    location=location,
                    employee_ranges=employee_ranges,
                    industry_id=industry_id,
                    industry_name=industry_name,
                    cookies_file=cookies_file,
                    headers_file=headers_file
                )
                
                self.stdout.write(self.style.SUCCESS(f"Completed {location} - {industry_name}: Found {results_count} organizations"))
                completed_in_run += 1
                    
            except Exception as e:
                # Log error if something goes wrong
                error_msg = f"Error processing {location} - {industry_name}: {str(e)}"
                self.stdout.write(self.style.ERROR(error_msg))
                
                # Update status to failed
                update_combination_status(location, industry_name, "failed")
                
                # Continue with next combination
                continue
            
            # Add a progress indicator
            self.stdout.write(f"Progress: {processed}/{pending_count} combinations processed ({(processed/pending_count)*100:.1f}%)")
            
            # Add a delay between different combinations
            if processed < pending_count:
                delay = random.randint(15, 20)  # random delay of 15-20 seconds between combinations
                self.stdout.write(f"Waiting {delay} seconds before next combination...")
                time.sleep(delay)
        
        # Calculate total run time
        total_run_time = time.time() - start_time
        hours, remainder = divmod(total_run_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        self.stdout.write(self.style.SUCCESS(f"\nAll {pending_count} filtered combinations processed!"))
        self.stdout.write(f"Total run time: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        self.stdout.write(f"Average time per combination: {total_run_time / pending_count:.2f} seconds")
        
        # Get updated counts
        updated_completed = combinations_collection.count_documents({"status": "completed"})
        updated_pending = combinations_collection.count_documents({"status": "pending"})
        updated_failed = combinations_collection.count_documents({"status": "failed"})
        
        self.stdout.write(f"Updated status breakdown:")
        self.stdout.write(f"  Completed: {updated_completed}")
        self.stdout.write(f"  Pending: {updated_pending}")
        self.stdout.write(f"  Failed: {updated_failed}")
        self.stdout.write(f"  In Progress: {combinations_collection.count_documents({'status': 'in_progress'})}")
        self.stdout.write("Data has been saved directly to MongoDB.")