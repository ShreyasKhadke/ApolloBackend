"""
Modified Apollo Fetcher script to extract specific fields and push directly to MongoDB
"""
import requests
import json
import time
import random
import string
import os
import csv
import pymongo
from itertools import product
from django.utils import timezone
from datetime import datetime
from django.conf import settings
from .combinations_generator import get_completed_combinations, generate_combinations, update_combination_status

def generate_random_string(length):
    """Generate a random string of fixed length"""
    characters = string.ascii_letters + string.digits
    return "".join(random.choice(characters) for _ in range(length))

def fetch_organization_details(org_ids, cookies, headers):
    """
    Fetch additional details for a list of organization IDs using the load_snippets endpoint
    """
    if not org_ids:
        return {"organizations": []}
        
    print(f"Fetching details for {len(org_ids)} organizations...")
    
    # Create cache key (current timestamp in milliseconds)
    cache_key = int(time.time() * 1000)
    
    # Prepare payload
    json_data = {
        'ids': org_ids,
        'cacheKey': cache_key,
    }
    
    # Make the request
    try:
        response = requests.post(
            'https://app.apollo.io/api/v1/organizations/load_snippets',
            cookies=cookies,
            headers=headers,
            json=json_data,
        )
        
        # Debug response
        print(f"Details API status code: {response.status_code}")
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching organization details: {response.text}")
            return {"organizations": []}
            
    except Exception as e:
        print(f"Exception when fetching organization details: {str(e)}")
        sys.exit(1)  # Exit with a non-zero status to indicate failure
        # return {"organizations": []}

def extract_organization_data(org):
    """
    Extract relevant fields from organization data.
    """
    additional_details = org.get("additional_details", {})
    current_time = timezone.now()

    return {
        "name": org.get("name", ""),
        "linkedin_url": org.get("linkedin_url", ""),
        "website_url": org.get("website_url", ""),
        "facebook_url": org.get("facebook_url", ""),
        "phone": org.get("sanitized_phone") or org.get("primary_phone", {}).get("sanitized_number", ""),
        "apollo_id": org.get("id", ""),
        "number_of_employees": additional_details.get("estimated_num_employees", 0),
        "industry": additional_details.get("industries", []),
        "keywords": additional_details.get("keywords", []),
        "address": additional_details.get("raw_address", ""),
        "created_at": current_time,
        "updated_at": current_time,
    }


def save_to_mongodb(extracted_data):
    """
    Save the extracted data to MongoDB
    """
    # Get the organization collection from the database
    organization_collection = settings.MONGO_DB['organization']
    industry_collection = settings.MONGO_DB['industry']
    keyword_collection = settings.MONGO_DB['keyword']
    
    # First, save industries and keywords if they don't exist
    for industry_name in extracted_data.get("industry", []):
        existing_industry = industry_collection.find_one({"name": industry_name})
        if not existing_industry:
            # Insert new industry
            industry_collection.insert_one({
                "name": industry_name,
                "created_at": timezone.now(),
                "updated_at": timezone.now()
            })
    
    for keyword_name in extracted_data.get("keywords", []):
        existing_keyword = keyword_collection.find_one({"name": keyword_name})
        if not existing_keyword:
            # Insert new keyword
            keyword_collection.insert_one({
                "name": keyword_name,
                "created_at": timezone.now(),
                "updated_at": timezone.now()
            })
    
    # Check if the organization already exists
    existing_org = organization_collection.find_one({"apollo_id": extracted_data["apollo_id"]})
    
    if existing_org:
        # Update the existing organization
        extracted_data["updated_at"] = timezone.now()
        organization_collection.update_one(
            {"apollo_id": extracted_data["apollo_id"]},
            {"$set": extracted_data}
        )
        print(f"Updated organization: {extracted_data['name']}")
        return existing_org["_id"]
    else:
        # Insert new organization
        result = organization_collection.insert_one(extracted_data)
        print(f"Inserted organization: {extracted_data['name']}")
        return result.inserted_id

def search_apollo_and_save_to_mongodb(location, employee_ranges, industry_id, industry_name, cookies_file="data/cookies.json", headers_file="data/headers.json"):
    """
    Search Apollo API and save relevant data to MongoDB
    """
    # First, update status to "in_progress"
    update_combination_status(location, industry_name, "in_progress")
    
    try:
        # Create session with all the required cookies
        session = requests.Session()
        
        def load_json_file(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)

        cookies = load_json_file(cookies_file)
        headers = load_json_file(headers_file)

        # Parse employee ranges from string
        employee_ranges_formatted = []
        for range_str in employee_ranges.split(", "):
            parts = range_str.split("-")
            if len(parts) == 2:
                employee_ranges_formatted.append(f"{parts[0]},{parts[1]}")
        
        # Generate a random session ID and seed like browser does
        search_session_id = f"{generate_random_string(8)}-{generate_random_string(4)}-{generate_random_string(4)}-{generate_random_string(4)}-{generate_random_string(12)}"
        random_seed = generate_random_string(10)
        
        print(f"Using search session ID: {search_session_id}")
        print(f"Using random seed: {random_seed}")
        
        # Store all organization IDs for detailed retrieval
        all_organization_ids = []
        total_results_found = 0
        
        # Paginate through results
        page = 1
        more_results = True
        
        while more_results:
            # Cache key is a timestamp in milliseconds - generate a new one for each request
            cache_key = int(time.time() * 1000)
            print(f"\nProcessing page {page}...")
            print(f"Using cache key: {cache_key}")
            
            # Create search payload with current page
            search_payload = {
                "page": page,
                "sort_ascending": False,
                "sort_by_field": "[none]",
                "organization_num_employees_ranges": employee_ranges_formatted,
                "organization_locations": [location],
                "organization_industry_tag_ids": [industry_id],
                "display_mode": "explorer_mode",
                "per_page": 25,
                "open_factor_names": [],
                "num_fetch_result": 25,
                "context": "companies-index-page",
                "show_suggestions": False,
                "include_account_engagement_stats": False,
                "finder_version": 2,
                "search_session_id": search_session_id,
                "ui_finder_random_seed": random_seed,
                "typed_custom_fields": [],
                "cacheKey": cache_key
            }
            
            print("Making search request...")
            # Make search request with the exact cookies and headers from browser
            search_response = requests.post(
                'https://app.apollo.io/api/v1/mixed_companies/search', 
                cookies=cookies, 
                headers=headers, 
                json=search_payload
            )
            
            # Debug response
            print(f"Search status code: {search_response.status_code}")
            
            # Try to parse JSON response
            try:
                search_results = search_response.json()
                
                # Check if we got any organizations back
                organizations = search_results.get('organizations', [])
                print(f"Found {len(organizations)} organizations on page {page}")
                
                # Collect organization IDs for the details API
                current_page_ids = []
                for org in organizations:
                    org_id = org.get('id')
                    if org_id and org_id not in all_organization_ids:
                        current_page_ids.append(org_id)
                        all_organization_ids.append(org_id)
                
                # Increment total found counter
                total_results_found += len(organizations)
                
                # Check if we've reached the end of results
                if len(organizations) < search_payload["per_page"]:
                    print(f"Reached end of results after page {page}")
                    more_results = False
                elif "pagination" in search_results and search_results["pagination"].get("has_next_page") is False:
                    print(f"Pagination indicates no more pages after page {page}")
                    more_results = False
                else:
                    # Continue to next page with a fixed delay
                    page += 1
                    delay = random.randint(15, 20)  # Fixed delay of 25 seconds between pages
                    print(f"Waiting {delay} seconds before next request...")
                    time.sleep(delay)
                    
                # Fetch details for current page and save to MongoDB
                if current_page_ids:
                    batch_details = fetch_organization_details(current_page_ids, cookies, headers)
                    if "organizations" in batch_details:
                        for org_detail in batch_details["organizations"]:
                            # Merge organization data from search and details
                            org_data = next((org for org in organizations if org.get("id") == org_detail.get("id")), {})
                            org_data["additional_details"] = org_detail
                            
                            # Extract relevant fields
                            extracted_data = extract_organization_data(org_data)
                            
                            # Save to MongoDB
                            save_to_mongodb(extracted_data)
                    
            except json.JSONDecodeError as e:
                print(f"Could not parse JSON response from search: {e}")
                more_results = False
            
            # Check for error response that might indicate we should stop
            if search_response.status_code != 200:
                print(f"Received non-200 status code ({search_response.status_code}). Stopping pagination.")
                more_results = False
        
        print(f"\nSearch completed! Found a total of {total_results_found} organizations")
        
        # Update combination status to completed with result count
        update_combination_status(location, industry_name, "completed", total_results_found)
        
        return total_results_found
        
    except Exception as e:
        # Update combination status to failed with error message
        update_combination_status(location, industry_name, "failed")
        print(f"Error during search: {str(e)}")
        raise e

def main():
    """Main function to run the Apollo Fetcher"""
    # Generate all combinations (this will also insert them into MongoDB if they don't exist)
    print("Generating and checking combinations in MongoDB...")
    print("This may take a while for large datasets...")
    combination_stats = generate_combinations()
    
    # Get combinations collection for direct query
    combinations_collection = settings.MONGO_DB['combinations']
    
    # Get pending combinations more efficiently by querying MongoDB directly
    print("Querying for pending combinations...")
    pending_cursor = combinations_collection.find(
        {"status": {"$in": ["pending", "failed"]}},
        {"location": 1, "employee_ranges": 1, "industry_id": 1, "industry_name": 1, "status": 1}
    )
    pending_combinations = list(pending_cursor)
    
    # Use counts from stats
    total_combinations = combination_stats["total"]
    pending_count = len(pending_combinations)
    completed_count = combination_stats["completed"]
    
    print(f"\nTotal combinations: {total_combinations}")
    print(f"Already completed: {completed_count}")
    print(f"Pending to process: {pending_count}")
    
    if pending_count == 0:
        print("All combinations have already been processed. Nothing to do.")
        exit(0)
    
    print(f"Processing {pending_count} pending combinations.")
    # Process each pending combination
    processed = 0
    for combo in pending_combinations:
        location = combo["location"]
        employee_ranges = combo["employee_ranges"]
        industry_id = combo["industry_id"]
        industry_name = combo["industry_name"]
        
        processed += 1
        print(f"\n{'='*50}")
        print(f"Processing combination {processed}/{pending_count} (Overall: {processed+completed_count}/{total_combinations})")
        print(f"Location: {location}")
        print(f"Industry: {industry_name}")
        print(f"{'='*50}\n")
        
        try:
            # Run search for this combination and save to MongoDB
            results_count = search_apollo_and_save_to_mongodb(location, employee_ranges, industry_id, industry_name)
            print(f"Completed {location} - {industry_name}: Found {results_count} organizations")
                
        except Exception as e:
            # Log error if something goes wrong
            print(f"Error processing {location} - {industry_name}: {str(e)}")
            # Continue with next combination
            continue
        
        # Add a fixed delay between different combinations (25 seconds)
        if processed < pending_count:
            delay = random.randint(15, 20)  # Exactly 25 seconds delay
            print(f"Waiting {delay} seconds before next combination...")
            time.sleep(delay)
    
    print(f"\nAll remaining {pending_count} combinations processed!")
    print(f"Total progress: {processed+completed_count}/{total_combinations} combinations")
    print("Data has been saved directly to MongoDB.")

if __name__ == "__main__":
    main()