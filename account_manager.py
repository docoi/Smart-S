"""
🔧 Account Manager Module - COMPLETE FIX
========================================
Fixed MillionVerifier to actually verify emails instead of assuming valid
"""

import os
import json
import time
import requests
from datetime import datetime
from apify_client import ApifyClient


class MillionVerifierManager:
    """💰 FIXED: MillionVerifier that actually verifies emails"""
    
    def __init__(self):
        self.api_key = os.getenv('MILLIONVERIFIER_API_KEY')
        if not self.api_key:
            print("⚠️ MILLIONVERIFIER_API_KEY not found in environment variables")
    
    def smart_verify_email(self, email, domain=None):
        """🧠 FIXED: Actually verify emails with MillionVerifier API"""
        
        if not self.api_key:
            print(f"      ⚠️ MillionVerifier API key not found - REJECTING {email}")
            return False
        
        try:
            # Use correct MillionVerifier API endpoint
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
                quality = result.get('quality', '').lower()
                result_status = result.get('result', '').lower()
                credits_remaining = result.get('credits', 0)
                
                print(f"      📊 MillionVerifier response: quality='{quality}', result='{result_status}'")
                print(f"      💳 Credits remaining: {credits_remaining}")
                
                # FIXED: Proper validation logic per MillionVerifier docs
                if quality == 'good' and result_status in ['ok', 'deliverable']:
                    print(f"      ✅ MillionVerifier: {email} is VALID - ACCEPT")
                    return True
                    
                elif quality == 'risky' and result_status == 'catch_all':
                    print(f"      ⚠️ MillionVerifier: {email} is on catch-all domain - ACCEPT (risky)")
                    return True
                        
                elif result_status in ['invalid', 'disposable', 'unknown'] or quality == 'bad':
                    print(f"      ❌ MillionVerifier: {email} is {result_status} - REJECT")
                    return False
                    
                else:
                    print(f"      ⚠️ MillionVerifier: {email} status '{quality}'/'{result_status}' - REJECT (unknown)")
                    return False
                    
            elif response.status_code == 401:
                print(f"      ❌ MillionVerifier: Invalid API key (401) - REJECTING {email}")
                return False
            elif response.status_code == 402:
                print(f"      ❌ MillionVerifier: No credits remaining (402) - REJECTING {email}")
                return False
            elif response.status_code == 429:
                print(f"      ⚠️ MillionVerifier: Rate limit exceeded (429) - retrying once...")
                time.sleep(2)
                return self.smart_verify_email(email, domain)  # Retry once
            else:
                print(f"      ❌ MillionVerifier API error: {response.status_code} - REJECTING {email}")
                print(f"      Response: {response.text[:200]}")
                return False
                
        except requests.exceptions.Timeout:
            print(f"      ⚠️ MillionVerifier: Request timeout - REJECTING {email}")
            return False
        except requests.exceptions.ConnectionError:
            print(f"      ❌ MillionVerifier: Connection error - REJECTING {email}")
            return False
        except Exception as e:
            print(f"      ❌ MillionVerifier error for {email}: {e} - REJECTING")
            return False
    
    def get_credits_balance(self):
        """📊 Get MillionVerifier credits balance"""
        if not self.api_key:
            return 0
            
        try:
            url = "https://api.millionverifier.com/api/v3/credits"
            params = {'api': self.api_key}
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                credits = data.get('credits', 0)
                return credits
            else:
                print(f"⚠️ Could not check MillionVerifier credits: {response.status_code}")
                return 0
                
        except Exception as e:
            print(f"⚠️ Error checking MillionVerifier credits: {e}")
            return 0


class ApifyAccountManager:
    """🎯 FORCE USE OF APIFY_TOKEN_1 ONLY"""
    
    def __init__(self):
        # 🎯 FORCE APIFY_TOKEN_1 ONLY
        self.primary_token = os.getenv('APIFY_TOKEN_1')
        if not self.primary_token:
            raise Exception("❌ APIFY_TOKEN_1 not found! This is required for the full LinkedIn plan.")
        
        print("🎯 FORCED: Using APIFY_TOKEN_1 only for all operations")
        print(f"💰 This account has the Full + email search subscription ($12 per 1000)")
        
        # Ensure output directory exists
        os.makedirs("output", exist_ok=True)
    
    def get_client_for_all_operations(self):
        """🎯 Get Apify client using APIFY_TOKEN_1 for ALL operations"""
        
        print("🎯 Getting client for all operations using APIFY_TOKEN_1...")
        
        # Create client
        client = ApifyClient(self.primary_token)
        client._account_info = {
            'name': 'APIFY_TOKEN_1',
            'token': self.primary_token
        }
        
        return client


def get_working_apify_client_part1():
    """🎯 Get APIFY_TOKEN_1 client for Part 1 (Website scraping)"""
    try:
        manager = ApifyAccountManager()
        return manager.get_client_for_all_operations()
    except Exception as e:
        print(f"❌ Failed to get APIFY_TOKEN_1 client: {e}")
        raise e


def get_working_apify_client_part2():
    """🎯 Get APIFY_TOKEN_1 client for Part 2 (LinkedIn scraping with email search)"""
    try:
        manager = ApifyAccountManager()
        return manager.get_client_for_all_operations()
    except Exception as e:
        print(f"❌ Failed to get APIFY_TOKEN_1 client: {e}")
        raise e