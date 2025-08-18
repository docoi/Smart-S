"""
🔧 Account Manager Module - FIXED MILLIONVERIFIER API
===================================================
✅ FIXED: Correct MillionVerifier API endpoints and response parsing
✅ FIXED: Real-time credit tracking with proper caching
✅ FIXED: Uses only APIFY_TOKEN_1 for all operations
"""

import os
import json
import time
import requests
from datetime import datetime
from apify_client import ApifyClient


class MillionVerifierManager:
    """💰 FIXED: Real-time MillionVerifier with correct API integration"""
    
    def __init__(self):
        self.api_key = os.getenv('MILLIONVERIFIER_API_KEY')
        self.credits_cache = None
        self.last_update = None
        
    def get_real_time_credits(self):
        """📊 FIXED: Get real-time MillionVerifier credits using correct endpoint"""
        
        try:
            # Cache for 30 seconds to avoid excessive API calls
            now = time.time()
            if (self.credits_cache is not None and 
                self.last_update is not None and 
                now - self.last_update < 30):
                return self.credits_cache
            
            # ✅ FIXED: Use correct credits endpoint
            url = "https://api.millionverifier.com/api/v3/credits"
            params = {'api': self.api_key}
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                # ✅ FIXED: Parse correct response structure
                credits = data.get('credits', 0)
                bulk_credits = data.get('bulk_credits', 0)
                
                self.credits_cache = credits
                self.last_update = now
                
                print(f"💳 MillionVerifier Credits: {credits} (Bulk: {bulk_credits})")
                return credits
            else:
                print(f"⚠️ MillionVerifier credits API error: {response.status_code}")
                print(f"Response: {response.text[:100]}")
                return self.credits_cache or 0
                
        except Exception as e:
            print(f"⚠️ Error checking MillionVerifier credits: {e}")
            return self.credits_cache or 0
    
    def smart_verify_email(self, email, domain=None):
        """🧠 FIXED: Smart MillionVerifier with correct API calls and response parsing"""
        
        if not self.api_key:
            print(f"      ⚠️ MillionVerifier API key not found - assuming valid")
            return True
        
        # Check credits before making API call
        credits_before = self.get_real_time_credits()
        if credits_before < 10:
            print(f"      ⚠️ Low MillionVerifier credits ({credits_before}) - assuming valid")
            return True
        
        try:
            # ✅ FIXED: Use correct verification endpoint
            url = "https://api.millionverifier.com/api/v3/"
            params = {
                'api': self.api_key,
                'email': email,
                'timeout': 10
            }
            
            print(f"      📡 MillionVerifier checking: {email}")
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                
                # ✅ FIXED: Parse response correctly according to API docs
                quality = result.get('quality', '').lower()
                result_status = result.get('result', '').lower()
                credits_after = result.get('credits', credits_before)
                
                # Update credits cache with real-time value from API response
                self.credits_cache = credits_after
                self.last_update = time.time()
                
                credits_used = credits_before - credits_after
                print(f"      📊 MillionVerifier response: quality='{quality}', result='{result_status}'")
                print(f"      💳 Credits: {credits_before} → {credits_after} (used: {credits_used})")
                
                # ✅ IMPROVED: Smart logic based on API documentation
                if quality == 'good' and result_status in ['ok', 'deliverable']:
                    print(f"      ✅ MillionVerifier: {email} is VALID")
                    return True
                    
                elif quality == 'risky':
                    if result_status == 'catch_all':
                        print(f"      ⚠️ MillionVerifier: {email} is on catch-all domain - ACCEPTING")
                        return True
                    elif result_status == 'unknown':
                        print(f"      ⚠️ MillionVerifier: {email} is risky/unknown - ACCEPTING")
                        return True
                    else:
                        print(f"      ❌ MillionVerifier: {email} is risky/{result_status} - REJECTING")
                        return False
                        
                elif result_status in ['invalid', 'disposable'] or quality == 'bad':
                    print(f"      ❌ MillionVerifier: {email} is {result_status} - REJECTING")
                    return False
                    
                else:
                    print(f"      ⚠️ MillionVerifier: {email} status '{quality}'/'{result_status}' - ACCEPTING")
                    return True
                    
            elif response.status_code == 401:
                print(f"      ❌ MillionVerifier: Invalid API key (401)")
                return False
            elif response.status_code == 402:
                print(f"      ❌ MillionVerifier: No credits remaining (402)")
                return False
            elif response.status_code == 429:
                print(f"      ⚠️ MillionVerifier: Rate limit exceeded (429)")
                time.sleep(1)
                return self.smart_verify_email(email, domain)  # Retry once
            else:
                print(f"      ⚠️ MillionVerifier API error: {response.status_code}")
                print(f"      Response: {response.text[:200]}")
                return True  # Assume valid on API errors
                
        except requests.exceptions.Timeout:
            print(f"      ⚠️ MillionVerifier: Request timeout for {email}")
            return True
        except requests.exceptions.ConnectionError:
            print(f"      ❌ MillionVerifier: Connection error for {email}")
            return True
        except Exception as e:
            print(f"      ❌ MillionVerifier error for {email}: {e}")
            return True


class ApifyAccountManager:
    """🎯 UPDATED: FORCE USE OF APIFY_TOKEN_1 ONLY with real-time monitoring"""
    
    def __init__(self):
        self.usage_file = "output/apify_usage_tracking.json"
        
        # 🎯 FORCE APIFY_TOKEN_1 ONLY
        self.primary_token = os.getenv('APIFY_TOKEN_1')
        if not self.primary_token:
            raise Exception("❌ APIFY_TOKEN_1 not found! This is required for the full LinkedIn plan.")
        
        print("🎯 FORCED: Using APIFY_TOKEN_1 only for all operations")
        print(f"💰 This account has the Full + email search subscription ($12 per 1000)")
        
        # Ensure output directory exists
        os.makedirs("output", exist_ok=True)
    
    def get_real_time_credit_usage(self):
        """📊 Get real-time credit usage for APIFY_TOKEN_1"""
        try:
            # Use EXACT same approach as your working script
            from urllib.request import Request, urlopen
            from urllib.error import HTTPError, URLError
            
            # EXACT same URL and method as your script
            LIMITS_URL = "https://api.apify.com/v2/users/me/limits"
            
            req = Request(LIMITS_URL, headers={"Authorization": f"Bearer {self.primary_token}"})
            
            with urlopen(req, timeout=20) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                data = json.loads(body)
                
                # Use EXACT same data extraction as your working script
                d = data.get("data", data)
                limits = d.get("limits", {})
                current = d.get("current", {})
                
                # Get monthlyUsageUsd EXACTLY like your script
                monthly_usage_usd = current.get("monthlyUsageUsd", 0.0)
                max_monthly_usd = limits.get("maxMonthlyUsageUsd", 5.0)
                
                remaining_usd = max_monthly_usd - monthly_usage_usd
                percentage = (monthly_usage_usd / max_monthly_usd * 100) if max_monthly_usd > 0 else 0
                
                # Also get compute units
                monthly_compute_units = current.get("monthlyActorComputeUnits", 0)
                max_compute_units = limits.get("maxMonthlyActorComputeUnits", 625)
                
                print(f"💰 APIFY_TOKEN_1: ${monthly_usage_usd:.3f}/${max_monthly_usd} (${remaining_usd:.3f} remaining)")
                print(f"📅 Monthly compute units: {monthly_compute_units}/{max_compute_units}")
                
                return {
                    'used': round(monthly_usage_usd, 3),
                    'limit': max_monthly_usd,
                    'remaining': round(remaining_usd, 3),
                    'percentage': round(percentage, 1),
                    'compute_units_used': monthly_compute_units,
                    'compute_units_limit': max_compute_units
                }
                
        except (HTTPError, URLError, Exception) as e:
            print(f"⚠️ Real-time credit check failed for APIFY_TOKEN_1: {e}")
            return None
    
    def get_client_for_all_operations(self):
        """🎯 Get Apify client using APIFY_TOKEN_1 for ALL operations"""
        
        print("🎯 Getting client for all operations using APIFY_TOKEN_1...")
        
        # Get real-time credit usage
        credits = self.get_real_time_credit_usage()
        
        if credits:
            remaining = credits['remaining']
            
            if remaining < 1.0:
                print(f"⚠️ WARNING: Low credits on APIFY_TOKEN_1 (${remaining} remaining)")
            else:
                print(f"✅ APIFY_TOKEN_1 ready: ${remaining} remaining")
        
        # Create client
        client = ApifyClient(self.primary_token)
        client._account_info = {
            'name': 'APIFY_TOKEN_1',
            'token': self.primary_token,
            'credits': credits
        }
        
        return client
    
    def log_usage(self, operation_type, cost_estimate=0):
        """📊 Log usage for monitoring"""
        try:
            log_file = "output/apify_usage_log.json"
            
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'account': 'APIFY_TOKEN_1',
                'operation': operation_type,
                'estimated_cost': cost_estimate,
                'real_time_credits': self.get_real_time_credit_usage()
            }
            
            # Load existing log
            log_data = []
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    log_data = json.load(f)
            
            # Add new entry
            log_data.append(log_entry)
            
            # Keep only last 50 entries
            log_data = log_data[-50:]
            
            # Save log
            with open(log_file, 'w') as f:
                json.dump(log_data, f, indent=2)
                
        except Exception as e:
            print(f"⚠️ Could not log usage: {e}")


def get_working_apify_client_part1():
    """🎯 Get APIFY_TOKEN_1 client for Part 1 (Website scraping)"""
    try:
        manager = ApifyAccountManager()
        client = manager.get_client_for_all_operations()
        manager.log_usage("part1_website_scraping", 0.05)
        return client
    except Exception as e:
        print(f"❌ Failed to get APIFY_TOKEN_1 client: {e}")
        raise e


def get_working_apify_client_part2():
    """🎯 Get APIFY_TOKEN_1 client for Part 2 (LinkedIn scraping with email search)"""
    try:
        manager = ApifyAccountManager()
        client = manager.get_client_for_all_operations()
        manager.log_usage("part2_linkedin_email_search", 12.00)  # Full + email search cost
        return client
    except Exception as e:
        print(f"❌ Failed to get APIFY_TOKEN_1 client: {e}")
        raise e