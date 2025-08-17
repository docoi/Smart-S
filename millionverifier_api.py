# verifier/millionverifier_api.py
# MillionVerifier API integration - much cheaper than Truelist!

import requests
import os
import time

def verify_email_millionverifier(email):
    """
    Verify email using MillionVerifier API
    Much cheaper: 2,000 credits for $4.90 vs Truelist 250/day free
    """
    api_key = os.getenv('MILLIONVERIFIER_API_KEY')
    
    if not api_key:
        print("      ⚠️  MillionVerifier API key not found")
        return False
    
    url = "https://api.millionverifier.com/api/v3/"
    params = {
        'api': api_key,
        'email': email,
        'timeout': 10
    }
    
    try:
        print(f"      📡 MillionVerifier checking: {email}")
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            quality = result.get('quality', '').lower()
            result_status = result.get('result', '').lower() 
            credits_remaining = result.get('credits', 0)
            
            print(f"      💳 Credits remaining: {credits_remaining}")
            is_valid = (quality == 'good' and result_status == 'deliverable')
            
            if is_valid:
                print(f"      ✅ MillionVerifier: {email} is valid ({quality}, {result_status})")
            else:
                print(f"      ❌ MillionVerifier: {email} is {quality} ({result_status})")
            
            if credits_remaining < 100:
                print(f"      ⚠️  Warning: Only {credits_remaining} credits remaining!")
            
            return is_valid
            
        elif response.status_code == 401:
            print(f"      ❌ MillionVerifier: Invalid API key (401)")
            return False
        elif response.status_code == 402:
            print(f"      ❌ MillionVerifier: No credits remaining (402)")
            return False
        elif response.status_code == 429:
            print(f"      ⚠️  MillionVerifier: Rate limit exceeded (429)")
            time.sleep(1)
            return verify_email_millionverifier(email)  # Retry once
        else:
            print(f"      ❌ MillionVerifier API error: {response.status_code}")
            print(f"      Response: {response.text[:200]}")
            return False
            
    except requests.exceptions.Timeout:
        print(f"      ⚠️  MillionVerifier: Request timeout")
        return False
    except requests.exceptions.ConnectionError:
        print(f"      ❌ MillionVerifier: Connection error")
        return False
    except Exception as e:
        print(f"      ❌ MillionVerifier error: {e}")
        return False

def get_millionverifier_balance():
    """Check remaining credits"""
    api_key = os.getenv('MILLIONVERIFIER_API_KEY')
    
    if not api_key:
        print("❌ MILLIONVERIFIER_API_KEY not found")
        return None
    
    url = "https://api.millionverifier.com/api/v3/"
    params = {
        'api': api_key,
        'email': 'test@example.com',  # Dummy email to check balance
        'timeout': 1
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            result = response.json()
            credits = result.get('credits', 0)
            print(f"💳 MillionVerifier Credits Remaining: {credits}")
            return credits
        else:
            print(f"❌ Could not check balance: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error checking balance: {e}")
        return None

# ✅ UPDATED: Better fallback logic
def verify_email_with_fallback(email):
    """Try MillionVerifier, then fall back to Truelist"""
    try:
        result = verify_email_millionverifier(email)
        if result is not None:
            return result
    except Exception as e:
        print(f"      ⚠️  MillionVerifier failed: {str(e)[:50]}...")
    
    try:
        print(f"      🔄 Falling back to Truelist...")
        from verifier.truelist_api import verify_email_truelist
        return verify_email_truelist(email)
    except Exception as e:
        print(f"      ⚠️  Truelist also failed: {str(e)[:50]}...")
    
    print(f"      ⚠️  All verification failed - assuming valid for testing")
    return True

# ✅ NEW FUNCTION: Dynamic verification method chooser
def choose_verification_method():
    """Choose which email verification service to use with proper fallback"""
    
    try:
        from verifier.millionverifier_api import get_millionverifier_balance, verify_email_with_fallback
        MILLIONVERIFIER_AVAILABLE = True
    except ImportError:
        MILLIONVERIFIER_AVAILABLE = False

    if MILLIONVERIFIER_AVAILABLE and os.getenv('MILLIONVERIFIER_API_KEY'):
        try:
            balance = get_millionverifier_balance()
            if balance and balance > 10:
                print(f"📧 Using MillionVerifier for email verification ({balance} credits remaining)")
                return verify_email_with_fallback
            else:
                print(f"⚠️  MillionVerifier has {balance or 0} credits - falling back to Truelist")
        except:
            print("⚠️  MillionVerifier error - falling back to Truelist")

    if os.getenv('TRUELIST_API_KEY'):
        print("📧 Using Truelist for email verification")
        from verifier.truelist_api import verify_email_truelist
        return verify_email_truelist
    elif os.getenv('RAPIDAPI_KEY') and os.getenv('RAPIDAPI_KEY') != 'your_rapidapi_key_here':
        print("📧 Using RapidAPI for email verification")
        from verifier.rapidapi_email import verify_email_rapidapi
        return verify_email_rapidapi
    else:
        print("⚠️  No email verification service configured - using mock verification")
        return lambda email: True

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    print("🔍 MillionVerifier API Testing")
    print("=" * 50)
    
    # Test the API
    test_millionverifier_api()
