#!/usr/bin/env python3
"""
Test script to verify if Open Trip Planner (OTP) removes cancelled trips from its response.

This script:
1. Fetches the current GTFS-RT feed to identify cancelled trips
2. Calls the OTP API with a test request
3. Checks if any cancelled trips appear in the OTP response
4. Reports the findings with detailed analysis
"""

import requests
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
import argparse
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

# Configure logging
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, 'otp_cancelled_trips_test.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class CancelledTripInfo:
    """Information about a cancelled trip"""
    trip_id: str
    route_id: str
    schedule_relationship: str
    timestamp: str

@dataclass
class OTPTripInfo:
    """Information about a trip found in OTP response"""
    trip_id: str
    route_id: str
    route_short_name: str
    route_long_name: str
    agency_name: str
    start_time: int
    end_time: int

class OTPCancelledTripsTester:
    """Test class for checking if OTP removes cancelled trips"""
    
    def __init__(self, otp_base_url: str = "http://0.0.0.0:8080"):
        self.otp_base_url = otp_base_url
        self.session = requests.Session()
        
        # Load environment variables
        load_dotenv()
        
        # Database configuration
        self.db_host = os.getenv('DB_HOST')
        self.db_port = os.getenv('DB_PORT', '5432')
        self.db_name = os.getenv('DB_NAME')
        self.db_user = os.getenv('DB_USER')
        self.db_password = os.getenv('DB_PASSWORD')
        
    def construct_trip_id(self, route_id, service_type_code, route_direction, schedule_trip_detail_id):
        """
        Construct trip ID using the standardized format (same as in poll_train_tripupdates.py).
        
        Args:
            route_id: The route ID
            service_type_code: The service type code
            route_direction: The route direction
            schedule_trip_detail_id: The schedule trip detail ID (trip_uniq_identifier)
        
        Returns:
            str: Constructed trip ID in format: {normalized_route_id}-{service_type_code}-{route_direction}-{schedule_trip_detail_id}
        """
        # Normalize route_id by replacing spaces with underscores
        normalized_route_id = str(route_id).replace(' ', '_')
        
        # Construct trip ID using the specified format
        trip_id = f"{normalized_route_id}-{service_type_code}-{route_direction}-{schedule_trip_detail_id}"
        
        return trip_id

    def get_cancelled_trips_from_database(self) -> List[CancelledTripInfo]:
        """Get cancelled trips directly from database using the same logic as poll_train_tripupdates.py"""
        if not all([self.db_host, self.db_name, self.db_user, self.db_password]):
            logger.warning("PostgreSQL configuration not complete, cannot fetch cancelled trips")
            return []
        
        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password
            )
            
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Query 1: Get all trips (without waybills)
                all_trips_query = """
                SELECT
                  "public"."bus_route"."route_id" AS "route_id",
                  "public"."bus_route"."route_name" AS "route_name",
                  "public"."bus_route"."route_number" AS "schedule_route_code",
                  "Bus Schedule Trip Detail - Route"."schedule_number" AS "schedule_number",
                  "Bus Schedule Trip Detail - Route"."schedule_trip_detail_id" AS "trip_uniq_identifier",
                  "public"."bus_route"."route_direction" AS "route_direction",
                  "Bus Schedule Trip Detail - Route"."start_time" AS "schedule_trip_start_time",
                  split_part(schedule_number, '-', 1) as "service_type_code"
                FROM
                  "public"."bus_route"
                  INNER JOIN "public"."bus_schedule_trip_detail" AS "Bus Schedule Trip Detail - Route" ON "public"."bus_route"."route_id" = "Bus Schedule Trip Detail - Route"."route_number_id"
                  INNER JOIN "public"."bus_schedule_trip" AS "bus_schedule_trip__via__schedule_trip_id" ON "Bus Schedule Trip Detail - Route"."schedule_trip_id" = "bus_schedule_trip__via__schedule_trip_id"."schedule_trip_id"
                WHERE
                  ("public"."bus_route"."status" = 'Active')
                  AND ("public"."bus_route"."deleted" = FALSE)
                  AND ("Bus Schedule Trip Detail - Route"."deleted" = FALSE)
                  AND ("bus_schedule_trip__via__schedule_trip_id"."deleted" = FALSE)
                  AND ("bus_schedule_trip__via__schedule_trip_id"."status" = 'Active')
                """
                
                cursor.execute(all_trips_query)
                all_trips = cursor.fetchall()
                logger.info(f"Fetched {len(all_trips)} total trips from database")
                
                # Query 2: Get trips happening today (with waybills)
                today_trips_query = """
                SELECT
                  "public"."bus_route"."route_id" AS "route_id",
                  "public"."bus_route"."route_name" AS "route_name",
                  "public"."bus_route"."route_number" AS "schedule_route_code",
                  "Bus Schedule Trip Detail - Route"."schedule_number" AS "schedule_number",
                  "Bus Schedule Trip Detail - Route"."schedule_trip_detail_id" AS "trip_uniq_identifier",
                  "public"."bus_route"."route_direction" AS "route_direction",
                  "Bus Schedule Trip Detail - Route"."start_time" AS "schedule_trip_start_time",
                  split_part(schedule_number, '-', 1) as "service_type_code"
                FROM
                  "public"."bus_route"
                  INNER JOIN "public"."bus_schedule_trip_detail" AS "Bus Schedule Trip Detail - Route" ON "public"."bus_route"."route_id" = "Bus Schedule Trip Detail - Route"."route_number_id"
                  INNER JOIN "public"."waybills"  ON "public"."waybills"."schedule_trip_id" = "Bus Schedule Trip Detail - Route"."schedule_trip_id"
                  INNER JOIN "public"."bus_schedule_trip" AS "bus_schedule_trip__via__schedule_trip_id" ON "Bus Schedule Trip Detail - Route"."schedule_trip_id" = "bus_schedule_trip__via__schedule_trip_id"."schedule_trip_id"
                WHERE
                  ("public"."bus_route"."status" = 'Active')
                  AND ("public"."bus_route"."deleted" = FALSE)
                  AND ("Bus Schedule Trip Detail - Route"."deleted" = FALSE)
                  AND ("bus_schedule_trip__via__schedule_trip_id"."deleted" = FALSE)
                  AND ("bus_schedule_trip__via__schedule_trip_id"."status" = 'Active')
                  AND "public"."waybills"."duty_date" = TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
                """
                
                cursor.execute(today_trips_query)
                today_trips = cursor.fetchall()
                logger.info(f"Fetched {len(today_trips)} trips happening today from database")
                
                # Convert to sets for comparison using constructed trip IDs
                all_trip_ids = set()
                today_trip_ids = set()
                
                # Construct trip IDs for all trips
                for trip in all_trips:
                    trip_id = self.construct_trip_id(
                        trip['route_id'], 
                        trip['service_type_code'], 
                        trip['route_direction'], 
                        trip['trip_uniq_identifier']
                    )
                    all_trip_ids.add(trip_id)
                
                # Construct trip IDs for today's trips
                for trip in today_trips:
                    trip_id = self.construct_trip_id(
                        trip['route_id'], 
                        trip['service_type_code'], 
                        trip['route_direction'], 
                        trip['trip_uniq_identifier']
                    )
                    today_trip_ids.add(trip_id)
                
                # Find cancelled trips (trips in all_trips but not in today_trips)
                cancelled_trip_ids = all_trip_ids - today_trip_ids
                logger.info(f"Found {len(cancelled_trip_ids)} cancelled trips")
                
                # Create cancelled trip info objects
                cancelled_trips = []
                for trip in all_trips:
                    trip_id = self.construct_trip_id(
                        trip['route_id'], 
                        trip['service_type_code'], 
                        trip['route_direction'], 
                        trip['trip_uniq_identifier']
                    )
                    
                    if trip_id in cancelled_trip_ids:
                        cancelled_trip = CancelledTripInfo(
                            trip_id=trip_id,
                            route_id=str(trip['route_id']),
                            schedule_relationship='CANCELED',
                            timestamp=str(int(time.time()))
                        )
                        cancelled_trips.append(cancelled_trip)
                        logger.info(f"Found cancelled trip: {trip_id} (Route: {trip['route_id']})")
                
                logger.info(f"Successfully fetched {len(cancelled_trips)} cancelled trips from database")
                return cancelled_trips
                
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL error: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching cancelled trips: {e}")
            return []
        finally:
            if 'conn' in locals():
                conn.close()
    
    def call_otp_api_paginated(self, from_place: str, to_place: str, time_str: str, 
                              date_str: str, mode: str = "BUS,WALK", 
                              num_itineraries: int = 100) -> Optional[Dict]:
        """Call OTP API with pagination support to fetch all results"""
        try:
            url = f"{self.otp_base_url}/otp/routers/default/plan"
            
            base_params = {
                'fromPlace': from_place,
                'toPlace': to_place,
                'time': time_str,
                'date': date_str,
                'mode': mode,
                'arriveBy': 'false',
                'wheelchair': 'false',
                'showIntermediateStops': 'true',
                'numIteneraries': str(num_itineraries),
                'additionalParameters': 'numIteneraries',
                'locale': 'en'
            }
            
            headers = {
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest',
                'Cookie': 'i18next=en-US'
            }
            
            logger.info(f"Calling OTP API with pagination support")
            logger.info(f"From: {from_place}")
            logger.info(f"To: {to_place}")
            logger.info(f"Time: {time_str}, Date: {date_str}")
            
            all_itineraries = []
            next_page_cursor = None
            page_count = 0
            max_pages = 10  # Safety limit to prevent infinite loops
            
            while page_count < max_pages:
                page_count += 1
                logger.info(f"Fetching page {page_count}...")
                
                # Add cursor to params if we have one
                params = base_params.copy()
                if next_page_cursor:
                    params['pageCursor'] = next_page_cursor
                
                response = self.session.get(url, params=params, headers=headers, timeout=60)
                response.raise_for_status()
                
                page_data = response.json()
                
                # Extract itineraries from this page
                if 'plan' in page_data and 'itineraries' in page_data['plan']:
                    page_itineraries = page_data['plan']['itineraries']
                    all_itineraries.extend(page_itineraries)
                    logger.info(f"Page {page_count}: Found {len(page_itineraries)} itineraries")
                else:
                    logger.warning(f"Page {page_count}: No itineraries found")
                    break
                
                # Check for next page cursor
                if 'nextPageCursor' in page_data and page_data['nextPageCursor']:
                    next_page_cursor = page_data['nextPageCursor']
                    logger.info(f"Page {page_count}: Next page cursor found, continuing...")
                else:
                    logger.info(f"Page {page_count}: No next page cursor, pagination complete")
                    break
            
            # Construct final response with all itineraries
            if all_itineraries and 'plan' in page_data:
                final_response = page_data.copy()
                final_response['plan']['itineraries'] = all_itineraries
                logger.info(f"Successfully fetched {len(all_itineraries)} total itineraries across {page_count} pages")
                return final_response
            else:
                logger.warning("No itineraries found in any page")
                return page_data if 'page_data' in locals() else None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling OTP API: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing OTP response JSON: {e}")
            return None
    
    def extract_otp_trips(self, otp_data: Dict) -> List[OTPTripInfo]:
        """Extract trip information from OTP response"""
        otp_trips = []
        
        if not otp_data or 'plan' not in otp_data:
            logger.warning("No plan found in OTP response")
            return otp_trips
        
        plan = otp_data['plan']
        if 'itineraries' not in plan:
            logger.warning("No itineraries found in OTP plan")
            return otp_trips
        
        for itinerary in plan['itineraries']:
            if 'legs' not in itinerary:
                continue
                
            for leg in itinerary['legs']:
                if leg.get('mode') == 'BUS' and leg.get('transitLeg', False):
                    otp_trip = OTPTripInfo(
                        trip_id=leg.get('tripId', ''),
                        route_id=leg.get('routeId', ''),
                        route_short_name=leg.get('routeShortName', ''),
                        route_long_name=leg.get('routeLongName', ''),
                        agency_name=leg.get('agencyName', ''),
                        start_time=leg.get('startTime', 0),
                        end_time=leg.get('endTime', 0)
                    )
                    otp_trips.append(otp_trip)
                    logger.debug(f"Found OTP trip: {otp_trip.trip_id} (Route: {otp_trip.route_id})")
        
        logger.info(f"Total OTP trips found: {len(otp_trips)}")
        return otp_trips
    
    def check_cancelled_trips_in_otp(self, cancelled_trips: List[CancelledTripInfo], 
                                   otp_trips: List[OTPTripInfo]) -> Tuple[List[str], List[str]]:
        """Check if any cancelled trips appear in OTP response"""
        cancelled_trip_ids = {trip.trip_id for trip in cancelled_trips}
        cancelled_route_ids = {trip.route_id for trip in cancelled_trips}
        
        found_cancelled_trip_ids = []
        found_cancelled_route_ids = []
        
        for otp_trip in otp_trips:
            # Remove "chennai_bus:" prefix from OTP trip ID for comparison
            clean_trip_id = otp_trip.trip_id
            if clean_trip_id.startswith("chennai_bus:"):
                clean_trip_id = clean_trip_id.replace("chennai_bus:", "", 1)
            
            # Check by trip ID (with and without prefix)
            if otp_trip.trip_id in cancelled_trip_ids or clean_trip_id in cancelled_trip_ids:
                found_cancelled_trip_ids.append(otp_trip.trip_id)
                logger.warning(f"FOUND CANCELLED TRIP BY ID: {otp_trip.trip_id} (clean: {clean_trip_id})")
            
            # Check by route ID (less precise but still relevant)
            if otp_trip.route_id in cancelled_route_ids:
                found_cancelled_route_ids.append(otp_trip.route_id)
                logger.warning(f"FOUND CANCELLED ROUTE: {otp_trip.route_id}")
        
        return found_cancelled_trip_ids, found_cancelled_route_ids
    
    def generate_report(self, cancelled_trips: List[CancelledTripInfo], 
                       otp_trips: List[OTPTripInfo],
                       found_cancelled_trip_ids: List[str],
                       found_cancelled_route_ids: List[str],
                       otp_data: Optional[Dict] = None) -> str:
        """Generate a detailed test report"""
        report = []
        report.append("=" * 80)
        report.append("OTP CANCELLED TRIPS TEST REPORT")
        report.append("=" * 80)
        report.append(f"Test Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Summary
        report.append("SUMMARY:")
        report.append(f"  Total cancelled trips in database: {len(cancelled_trips)}")
        report.append(f"  Total trips found in OTP response: {len(otp_trips)}")
        total_itineraries = len(otp_data.get('plan', {}).get('itineraries', [])) if otp_data else 0
        report.append(f"  Total itineraries in OTP response: {total_itineraries}")
        report.append(f"  Cancelled trip IDs found in OTP: {len(found_cancelled_trip_ids)}")
        report.append(f"  Cancelled route IDs found in OTP: {len(found_cancelled_route_ids)}")
        report.append("")
        
        # Test result
        if found_cancelled_trip_ids or found_cancelled_route_ids:
            report.append("❌ TEST RESULT: FAILED - OTP is NOT properly removing cancelled trips")
            report.append("")
            report.append("DETAILS:")
            if found_cancelled_trip_ids:
                report.append(f"  Cancelled trip IDs found in OTP: {found_cancelled_trip_ids}")
            if found_cancelled_route_ids:
                report.append(f"  Cancelled route IDs found in OTP: {found_cancelled_route_ids}")
        else:
            report.append("✅ TEST RESULT: PASSED - OTP is properly removing cancelled trips")
        
        report.append("")
        
        # Add trip IDs in comma-separated format at the end
        report.append("")
        report.append("TRIP IDS SUMMARY:")
        report.append("=" * 50)
        
        if cancelled_trips:
            cancelled_trip_ids = [trip.trip_id for trip in cancelled_trips]
            report.append(f"Cancelled trip IDs ({len(cancelled_trip_ids)}):")
            report.append(",".join(cancelled_trip_ids))
            report.append("")
        
        if otp_trips:
            otp_trip_ids = [trip.trip_id for trip in otp_trips]
            report.append(f"OTP trip IDs ({len(otp_trip_ids)}):")
            report.append(",".join(otp_trip_ids))
            report.append("")
        
        if found_cancelled_trip_ids:
            report.append(f"Found cancelled trip IDs in OTP ({len(found_cancelled_trip_ids)}):")
            report.append(",".join(found_cancelled_trip_ids))
            report.append("")
        
        return "\n".join(report)
    
    def run_test(self, from_place: str, to_place: str, time_str: str, 
                date_str: str, mode: str = "BUS,WALK", 
                num_itineraries: int = 100) -> bool:
        """Run the complete test"""
        logger.info("Starting OTP cancelled trips test")
        
        # Step 1: Get cancelled trips from database
        logger.info("Step 1: Fetching cancelled trips from database...")
        cancelled_trips = self.get_cancelled_trips_from_database()
        if not cancelled_trips:
            logger.warning("No cancelled trips found in database. Test will continue but may not be meaningful.")
        
        # Step 2: Call OTP API with pagination
        logger.info("Step 2: Calling OTP API with pagination...")
        otp_data = self.call_otp_api_paginated(from_place, to_place, time_str, date_str, mode, num_itineraries)
        if not otp_data:
            logger.error("Failed to call OTP API. Test aborted.")
            return False
        
        # Step 3: Extract OTP trips
        logger.info("Step 3: Extracting OTP trips...")
        otp_trips = self.extract_otp_trips(otp_data)
        
        # Step 4: Check for cancelled trips in OTP response
        logger.info("Step 4: Checking for cancelled trips in OTP response...")
        found_cancelled_trip_ids, found_cancelled_route_ids = self.check_cancelled_trips_in_otp(
            cancelled_trips, otp_trips
        )
        
        # Step 5: Generate and display report
        logger.info("Step 5: Generating test report...")
        report = self.generate_report(
            cancelled_trips, otp_trips, 
            found_cancelled_trip_ids, found_cancelled_route_ids, otp_data
        )
        
        print(report)
        
        # Save report to file in the test directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        report_file = os.path.join(script_dir, 'otp_cancelled_trips_report.txt')
        with open(report_file, 'w') as f:
            f.write(report)
        
        # Trip IDs are already included in the main report file
        
        logger.info(f"Test completed. Report saved to '{report_file}'")
        
        # Return True if test passed (no cancelled trips found in OTP)
        return len(found_cancelled_trip_ids) == 0 and len(found_cancelled_route_ids) == 0

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Test if OTP removes cancelled trips')
    parser.add_argument('--otp-url', default='http://0.0.0.0:8080',
                       help='OTP API base URL (default: http://0.0.0.0:8080)')
    parser.add_argument('--from-place', 
                       default='NANDANAM METRO (bfcf9b7b9d25bf3659a12a14cc9e9362)::13.0309,80.23999',
                       help='From place for OTP request')
    parser.add_argument('--to-place',
                       default='Alandur Metro (SAL|0231)::13.003722,80.2015436',
                       help='To place for OTP request')
    parser.add_argument('--time', default='7:38pm',
                       help='Time for OTP request (default: 7:38pm)')
    parser.add_argument('--date', default='09-17-2025',
                       help='Date for OTP request (default: 09-17-2025)')
    parser.add_argument('--mode', default='BUS,WALK',
                       help='Transportation modes (default: BUS,WALK)')
    parser.add_argument('--num-itineraries', type=int, default=100,
                       help='Number of itineraries to request (default: 100)')
    
    args = parser.parse_args()
    
    # Create tester instance
    tester = OTPCancelledTripsTester(otp_base_url=args.otp_url)
    
    # Run test
    success = tester.run_test(
        from_place=args.from_place,
        to_place=args.to_place,
        time_str=args.time,
        date_str=args.date,
        mode=args.mode,
        num_itineraries=args.num_itineraries
    )
    
    # Exit with appropriate code
    exit(0 if success else 1)

if __name__ == "__main__":
    main()