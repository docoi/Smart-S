"""
🔧 Account Manager Module - ENHANCED VERSION
============================================
✅ FIXED: Added get_working_apify_client_part2() method
✅ FIXED: Enhanced credit monitoring for both Part 1 and Part 2
✅ FIXED: Better account rotation logic
"""

import os
import json
import time
import requests
from datetime import datetime
from apify_client import ApifyClient


class MillionVerifierManager:
    """💰 Real-time MillionVerifier credit tracking and smart catch-all logic"""
    
    def __init__(self):
        self.api_key = os.getenv('MILLIONVERIFIER_API_KEY')
        self.credits_cache = None
        self.last_update = None
        
    def get_real_time_credits(self):
        """📊 Get real-time MillionVerifier credits with caching"""
        
        try:
            # Cache for 30 seconds to avoid excessive API calls
            now = time.time()
            if (self.credits_cache is not None and 
                self.last_update is not None and 
                now - self.last_update < 30):
                return self.credits_cache
            
            url = "https://api.millionverifier.com/api/v3/credits"
            params = {'api': self.api_key}
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                credits = data.get('credits', 0)
                
                self.credits_cache = credits
                self.last_update = now
                
                print(f"💳 MillionVerifier Credits: {credits}")
                return credits
            else:
                print(f"⚠️ MillionVerifier credits API error: {response.status_code}")
                return self.credits_cache or 0
                
        except Exception as e:
            print(f"⚠️ Error checking MillionVerifier credits: {e}")
            return self.credits_cache or 0
    
    def smart_verify_email(self, email, domain=None):
        """🧠 FIXED: Smart MillionVerifier with real-time credits and catch-all intelligence"""
        
        if not self.api_key:
            print(f"      ⚠️ MillionVerifier API key not found - assuming valid")
            return True
        
        # Check credits before making API call
        credits_before = self.get_real_time_credits()
        if credits_before < 10:
            print(f"      ⚠️ Low MillionVerifier credits ({credits_before}) - assuming valid")
            return True
        
        try:
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
                credits_after = result.get('credits', credits_before)
                
                # Update credits cache with real-time value from API response
                self.credits_cache = credits_after
                self.last_update = time.time()
                
                credits_used = credits_before - credits_after
                print(f"      📊 MillionVerifier response: quality='{quality}', result='{result_status}'")
                print(f"      💳 Credits: {credits_after} (used: {credits_used})")
                
                # 🧠 SMART LOGIC: Accept ANY email source, not just specific ones
                if quality == 'good' and result_status in ['ok', 'deliverable']:
                    print(f"      ✅ MillionVerifier: {email} is valid - ACCEPT")
                    return True
                    
                elif quality == 'risky' and result_status == 'catch_all':
                    print(f"      ⚠️ MillionVerifier: {email} is on catch-all domain - ACCEPT")
                    # NEW: Accept ALL emails on catch-all domains
                    return True
                        
                elif result_status in ['invalid', 'disposable'] or quality == 'bad':
                    print(f"      ❌ MillionVerifier: {email} is {result_status} - REJECT")
                    return False
                    
                else:
                    print(f"      ⚠️ MillionVerifier: {email} status '{quality}'/'{result_status}' - ACCEPT")
                    # NEW: Accept unknown statuses
                    return True
            else:
                print(f"      ⚠️ MillionVerifier API error: {response.status_code} - assuming valid")
                return True
                
        except Exception as e:
            print(f"      ⚠️ MillionVerifier error for {email}: {e} - assuming valid")
            return True


class ApifyAccountManager:
    """Smart Apify account management with REAL-TIME credit monitoring and automatic switching"""
    
    def __init__(self):
        self.usage_file = "output/apify_usage_tracking.json"
        self.accounts = self.load_accounts()
        self.usage_data = self.load_usage_data()
        
        # Ensure output directory exists
        os.makedirs("output", exist_ok=True)
    
    def load_accounts(self):
        """Load all available Apify accounts from environment"""
        accounts = []
        for i in range(1, 11):  # Support up to 10 accounts
            token = os.getenv(f'APIFY_TOKEN_{i}') or os.getenv(f'APIFY_API_TOKEN_{i}')
            if token:
                accounts.append({
                    'id': i,
                    'token': token,
                    'name': f'Account_{i}',
                    'active': True
                })
        
        # Fallback to original token names
        main_token = os.getenv('APIFY_TOKEN') or os.getenv('APIFY_API_TOKEN')
        if main_token and not accounts:
            accounts.append({
                'id': 0,
                'token': main_token,
                'name': 'Main_Account',
                'active': True
            })
        
        print(f"📊 Loaded {len(accounts)} Apify accounts for rotation")
        return accounts
    
    def get_real_time_credit_usage(self, account):
        """FIXED: Get real-time credit usage using EXACT same method as your working script"""
        try:
            # Use EXACT same approach as your working script
            from urllib.request import Request, urlopen
            from urllib.error import HTTPError, URLError
            
            # EXACT same URL and method as your script
            LIMITS_URL = "https://api.apify.com/v2/users/me/limits"
            
            req = Request(LIMITS_URL, headers={"Authorization": f"Bearer {account['token']}"})
            
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
                
                print(f"   💰 {account['name']}: ${monthly_usage_usd:.3f}/${max_monthly_usd} (${remaining_usd:.3f} remaining)")
                print(f"      📅 Monthly cost: ${monthly_usage_usd:.3f}")
                
                return {
                    'used': round(monthly_usage_usd, 3),
                    'limit': max_monthly_usd,
                    'remaining': round(remaining_usd, 3),
                    'percentage': round(percentage, 1),
                    'compute_units_used': monthly_compute_units,
                    'compute_units_limit': max_compute_units
                }
                
        except (HTTPError, URLError, Exception) as e:
            print(f"   ⚠️ Real-time credit check failed for {account['name']}: {e}")
            return None
    
    def get_best_account_part1(self, credit_threshold=4.85):
        """Get best account for Part 1 with REAL-TIME credit monitoring and threshold switching"""
        print(f"🔍 Part 1: Checking accounts for credit availability (threshold: ${credit_threshold})...")
        
        available_accounts = []
        
        for account in self.accounts:
            if not account['active']:
                continue
            
            # Get real-time credit usage using FIXED method
            real_time_credits = self.get_real_time_credit_usage(account)
            
            if real_time_credits:
                remaining = real_time_credits['remaining']
                used = real_time_credits['used']
                limit = real_time_credits['limit']
                
                # Check if account has enough credits above threshold
                threshold_remaining = limit - credit_threshold
                if remaining <= threshold_remaining:
                    print(f"   ⚠️ {account['name']}: Below threshold (${remaining} <= ${threshold_remaining}), skipping")
                    continue
                
                # Test if account is working
                if self.test_account_working(account):
                    available_accounts.append({
                        'account': account,
                        'credits': real_time_credits,
                        'remaining': remaining
                    })
                    print(f"   ✅ {account['name']}: Available (${remaining} remaining, above ${credit_threshold} threshold)")
                else:
                    print(f"   ❌ {account['name']}: Not responding")
            else:
                print(f"   ❌ {account['name']}: Could not check credits")
        
        if not available_accounts:
            print(f"❌ No accounts with credits above ${credit_threshold} threshold found!")
            return None
        
        # Sort by most credits remaining
        available_accounts.sort(key=lambda x: x['remaining'], reverse=True)
        best = available_accounts[0]
        
        print(f"🎯 Part 1 Selected: {best['account']['name']} (${best['remaining']} remaining)")
        
        # Log detailed usage for monitoring
        self._log_credit_usage(best['account'], best['credits'])
        
        return best['account']
    
    def get_best_account_part2(self, credit_threshold=4.85):
        """🔧 FIXED: Get best account for Part 2 (LinkedIn) with REAL-TIME credit monitoring"""
        print(f"🔍 Part 2: Checking accounts for LinkedIn scraping (threshold: ${credit_threshold})...")
        
        available_accounts = []
        
        for account in self.accounts:
            if not account['active']:
                continue
            
            # Get real-time credit usage using FIXED method
            real_time_credits = self.get_real_time_credit_usage(account)
            
            if real_time_credits:
                remaining = real_time_credits['remaining']
                used = real_time_credits['used']
                limit = real_time_credits['limit']
                
                # Check if account has enough credits above threshold
                threshold_remaining = limit - credit_threshold
                if remaining <= threshold_remaining:
                    print(f"   ⚠️ {account['name']}: Below threshold (${remaining} <= ${threshold_remaining}), skipping")
                    continue
                
                # Test if account is working
                if self.test_account_working(account):
                    available_accounts.append({
                        'account': account,
                        'credits': real_time_credits,
                        'remaining': remaining
                    })
                    print(f"   ✅ {account['name']}: Available for Part 2 (${remaining} remaining)")
                else:
                    print(f"   ❌ {account['name']}: Not responding")
            else:
                print(f"   ❌ {account['name']}: Could not check credits")
        
        if not available_accounts:
            print(f"❌ No accounts with credits above ${credit_threshold} threshold found for Part 2!")
            return None
        
        # Sort by most credits remaining
        available_accounts.sort(key=lambda x: x['remaining'], reverse=True)
        best = available_accounts[0]
        
        print(f"🎯 Part 2 Selected: {best['account']['name']} (${best['remaining']} remaining)")
        
        # Log detailed usage for monitoring
        self._log_credit_usage(best['account'], best['credits'])
        
        return best['account']
    
    def load_usage_data(self):
        """Load usage tracking data from file"""
        try:
            if os.path.exists(self.usage_file):
                with open(self.usage_file, 'r') as f:
                    data = json.load(f)
                print(f"📈 Loaded usage data for {len(data)} accounts")
                return data
        except Exception as e:
            print(f"⚠️ Error loading usage data: {e}")
        
        # Initialize empty usage data
        return {str(acc['id']): {'runs_used': 0, 'runs_limit': 8, 'last_reset': datetime.now().strftime('%Y-%m')} 
                for acc in self.accounts}
    
    def save_usage_data(self):
        """Save usage tracking data to file"""
        try:
            os.makedirs("output", exist_ok=True)
            with open(self.usage_file, 'w') as f:
                json.dump(self.usage_data, f, indent=2)
        except Exception as e:
            print(f"⚠️ Error saving usage data: {e}")
    
    def test_account_working(self, account):
        """Simple test to see if account is working"""
        try:
            client = ApifyClient(account['token'])
            
            # Try a simple API call that doesn't cost runs - list actors
            response = client.actors().list()
            
            return response is not None
            
        except Exception as e:
            error_msg = str(e).lower()
            if "limit exceeded" in error_msg or "free user" in error_msg:
                return False  # Account exhausted
            else:
                print(f"   ⚠️ {account['name']}: API Error - {e}")
                return False
    
    def record_usage(self, account, success=True):
        """Record usage for an account"""
        account_id = str(account['id'])
        
        if account_id not in self.usage_data:
            self.usage_data[account_id] = {'runs_used': 0, 'runs_limit': 8, 'last_reset': datetime.now().strftime('%Y-%m')}
        
        if success:
            self.usage_data[account_id]['runs_used'] += 1
            print(f"📊 {account['name']}: Updated LinkedIn calls to {self.usage_data[account_id]['runs_used']}/{self.usage_data[account_id]['runs_limit']}")
        
        self.save_usage_data()
    
    def _log_credit_usage(self, account, credits):
        """Log detailed credit usage for monitoring"""
        try:
            log_file = "output/credit_monitoring_log.json"
            
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'account': account['name'],
                'real_time_usage': credits,
                'usd_used': credits.get('used', 0),
                'usd_limit': credits.get('limit', 5.0),
                'usd_remaining': credits.get('remaining', 0),
                'compute_units_used': credits.get('compute_units_used', 0),
                'compute_units_limit': credits.get('compute_units_limit', 0)
            }
            
            # Load existing log
            log_data = []
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    log_data = json.load(f)
            
            # Add new entry
            log_data.append(log_entry)
            
            # Keep only last 100 entries
            log_data = log_data[-100:]
            
            # Save log
            os.makedirs("output", exist_ok=True)
            with open(log_file, 'w') as f:
                json.dump(log_data, f, indent=2)
                
        except Exception as e:
            print(f"   ⚠️ Could not log credit usage: {e}")
    
    def get_client_part1(self):
        """Get working Apify client for Part 1 (credit-based)"""
        best_account = self.get_best_account_part1()
        
        if not best_account:
            raise Exception("No accounts with available credits for Part 1")
        
        client = ApifyClient(best_account['token'])
        client._account_info = best_account
        client._part = "part1"
        
        return client

    def get_client_part2(self):
        """🔧 FIXED: Get working Apify client for Part 2 (LinkedIn) with credit management"""
        best_account = self.get_best_account_part2()
        
        if not best_account:
            raise Exception("No accounts with available credits for Part 2")
        
        client = ApifyClient(best_account['token'])
        client._account_info = best_account
        client._part = "part2"
        
        return client


def get_working_apify_client_part1():
    """Get working Apify client for Part 1 using credit management"""
    
    try:
        manager = ApifyAccountManager()
        return manager.get_client_part1()
    except Exception as e:
        print(f"❌ Failed to get working Apify client for Part 1: {e}")
        # Fallback to original method
        apify_token = os.getenv('APIFY_API_TOKEN') or os.getenv('APIFY_TOKEN')
        if apify_token:
            print("🔥 Using fallback token for Part 1")
            return ApifyClient(apify_token)
        raise e


def get_working_apify_client_part2():
    """🔧 FIXED: Get working Apify client for Part 2 using credit management"""
    
    try:
        manager = ApifyAccountManager()
        return manager.get_client_part2()
    except Exception as e:
        print(f"❌ Failed to get working Apify client for Part 2: {e}")
        # Fallback to original method
        apify_token = os.getenv('APIFY_API_TOKEN') or os.getenv('APIFY_TOKEN')
        if apify_token:
            print("🔥 Using fallback token for Part 2")
            return ApifyClient(apify_token)
        raise e