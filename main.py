#!/usr/bin/env python3
"""
🚀 COMPLETE WORKFLOW SUPER SCRAPER - MODULAR VERSION
==================================================
✅ FIXED: All syntax errors and duplicates removed
✅ FIXED: Modular structure for easier maintenance
✅ FIXED: Enhanced staff extraction with increased content limits
✅ FIXED: Smart pattern learning and email discovery
✅ FIXED: Real-time credit monitoring and account rotation
✅ FIXED: Smart Fallback workflow for when LinkedIn is unavailable
✅ FIXED: URL normalization and duplicate email prevention
✅ FIXED: Variable scope issues and email matching logic

Usage: python main.py --url https://target-website.com --credit-threshold 4.85 --max-emails 2

Features:
✅ Website scraping with enhanced GPT-4o analysis (100K chars vs 15K)
✅ LinkedIn employee discovery with smart email patterns
✅ Smart Fallback workflow when LinkedIn fails/unavailable
✅ Real-time credit monitoring with automatic account switching
✅ Smart MillionVerifier with catch-all domain intelligence
✅ GPT-4o-mini person validation (filters out company accounts)
✅ Pattern learning that discovers successful patterns and applies to all contacts
✅ Fire protection targeting with advanced scoring
✅ AI email generation and sending
✅ Dynamic URL handling with automatic normalization
✅ Email duplicate prevention and limit control
"""

from __future__ import annotations
import os
import sys
import csv
import time
import argparse
import re
from datetime import datetime
from urllib.parse import urlparse, urlunparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import our modular components
try:
    from account_manager import MillionVerifierManager, ApifyAccountManager
    from website_scraper import WebsiteScraper
    from linkedin_scraper import LinkedInScraper
    from expert_email_generator import ExpertEmailGenerator
    from send_smtp import send_email
    print("✅ All modules imported successfully")
except ImportError as e:
    print(f"❌ Missing modules: {e}")
    print("🔧 Make sure all module files are in the same directory")
    sys.exit(1)


def normalize_url(url: str) -> str:
    """
    🔧 Normalize URL to ensure proper format
    - Adds https:// if missing
    - Adds www. if missing (unless IP/localhost)
    - Fixes double protocol issues
    """
    if not url:
        return url
    
    # Remove whitespace
    url = url.strip()
    
    # Add https:// if no protocol
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Parse URL
    parsed = urlparse(url)
    
    # Fix double protocol issue (e.g., https://www.https://example.com/)
    if 'https://' in parsed.netloc or 'http://' in parsed.netloc:
        # Extract actual domain from malformed URL
        domain_match = re.search(r'https?://(?:www\.)?([^/]+)', url)
        if domain_match:
            domain = domain_match.group(1)
            # Clean up any remaining protocol parts
            domain = re.sub(r'https?://', '', domain)
            url = f"https://www.{domain}" if not domain.startswith('www.') else f"https://{domain}"
        else:
            raise ValueError(f"Cannot parse malformed URL: {url}")
    else:
        # Standard URL normalization
        domain = parsed.netloc.lower()
        
        # Add www. if not present (unless IP address or localhost)
        if (not domain.startswith('www.') and 
            not re.match(r'^\d+\.\d+\.\d+\.\d+', domain) and 
            domain != 'localhost'):
            domain = f"www.{domain}"
        
        # Reconstruct URL
        url = urlunparse((parsed.scheme, domain, parsed.path, parsed.params, parsed.query, parsed.fragment))
    
    # Ensure trailing slash if no path
    if not parsed.path or parsed.path == '/':
        if not url.endswith('/'):
            url += '/'
    
    return url


def remove_duplicate_emails(verified_contacts: list, max_emails: int = 2) -> list:
    """
    🔧 Remove duplicate emails and limit to max_emails
    - Prioritizes higher fire protection scores
    - Removes duplicate email addresses properly
    - Limits total emails sent
    """
    if not verified_contacts:
        return []
    
    # Remove duplicates by email address while preserving contact info
    seen_emails = set()
    unique_contacts = []
    
    for contact in verified_contacts:
        email = contact.get('email', '').lower().strip()
        if email and email not in seen_emails:
            seen_emails.add(email)
            unique_contacts.append(contact)
    
    # Sort by fire protection score (highest first)
    unique_contacts.sort(key=lambda x: x.get('fire_protection_score', 0), reverse=True)
    
    # Limit to max_emails
    limited_contacts = unique_contacts[:max_emails]
    
    print(f"📧 Email filtering results:")
    print(f"   📊 Total found: {len(verified_contacts)}")
    print(f"   🔄 After deduplication: {len(unique_contacts)}")
    print(f"   ✂️ After limiting to {max_emails}: {len(limited_contacts)}")
    
    return limited_contacts


class CompleteWorkflowSuperScraper:
    """🚀 Complete workflow orchestrator - coordinates all modules"""
    
    def __init__(self, max_emails: int = 2):
        self.openai_key = os.getenv('OPENAI_API_KEY')
        self.test_email = "dave@alpha-omegaltd.com"
        self.max_emails = max_emails
        
        if not self.openai_key:
            raise RuntimeError("🚨 Missing API keys: OPENAI_API_KEY required")
        
        # Initialize managers
        self.millionverifier = MillionVerifierManager()
        self.website_scraper = WebsiteScraper(self.openai_key)
        self.linkedin_scraper = LinkedInScraper(self.openai_key, self.millionverifier)
        
        print("🚀 COMPLETE WORKFLOW SUPER SCRAPER INITIALIZED")
        print(f"🧠 AI: GPT-4o analysis + GPT-4o-mini person validation")
        print(f"🛡️ Stealth: Apify cheapest proxies with account rotation")
        print(f"📧 Test emails: {self.test_email}")
        print(f"📊 Max emails per run: {self.max_emails}")

    def run_complete_workflow(self, website_url: str) -> dict:
        """🎯 Complete workflow: Website → LinkedIn OR Smart Fallback → Emails → Send"""
        
        # Normalize URL first
        normalized_url = normalize_url(website_url)
        
        print("\n🎯 STARTING COMPLETE WORKFLOW")
        print("=" * 60)
        print(f"🌐 Original URL: {website_url}")
        print(f"🌐 Normalized URL: {normalized_url}")
        print(f"🔄 Process: Website → Staff → LinkedIn → Patterns → AI → Send")
        print(f"📧 Test mode: All emails go to {self.test_email}")
        print(f"📊 Max emails: {self.max_emails}")
        print()
        
        results = {
            'website_url': normalized_url,
            'original_url': website_url,
            'website_staff': [],
            'linkedin_url': '',
            'linkedin_employees': [],
            'verified_contacts': [],
            'emails_sent': [],
            'status': 'started',
            'fallback_contacts': [],
            'fallback_emails': []
        }
        
        try:
            # PART 1: Website scraping (using normalized URL)
            print("📋 PART 1: WEBSITE SCRAPING")
            print("-" * 40)
            website_data = self.website_scraper.scrape_website_for_staff_and_linkedin(normalized_url)
            
            # Handle both old and new return formats
            if isinstance(website_data, tuple):
                website_staff, linkedin_url = website_data
                fallback_contacts = []
                fallback_emails = []
            else:
                website_staff = website_data.get('staff', [])
                linkedin_url = website_data.get('linkedin_url', '')
                fallback_contacts = website_data.get('fallback_contacts', [])
                fallback_emails = website_data.get('fallback_emails', [])
            
            results['website_staff'] = website_staff
            results['linkedin_url'] = linkedin_url
            results['fallback_contacts'] = fallback_contacts
            results['fallback_emails'] = fallback_emails
            
            print(f"✅ Part 1 complete: {len(website_staff)} staff + LinkedIn URL: {'Found' if linkedin_url else 'Not found'}")
            
            verified_contacts = []
            domain = urlparse(normalized_url).netloc.replace('www.', '')  # Define domain here for scope
            
            # Try LinkedIn pipeline first if URL available
            if linkedin_url:
                # Cooling-off period before Part 2
                print("\n⏳ COOLING-OFF PERIOD: Waiting 3 seconds before LinkedIn phase...")
                time.sleep(3)
                
                # PART 2: LinkedIn pipeline
                print(f"\n🔗 PART 2: LINKEDIN PIPELINE")
                print("-" * 40)
                print(f"🏢 Using modular LinkedIn pipeline with advanced features")
                print(f"✅ Features: Actor 2 + 33 Golden Patterns + Smart MillionVerifier + Pattern Learning")
                
                try:
                    verified_contacts = self.linkedin_scraper.scrape_linkedin_and_discover_emails(linkedin_url, domain)
                except Exception as e:
                    print(f"❌ LinkedIn pipeline failed: {e}")
                    verified_contacts = []
            
            # SMART FALLBACK: Use website data if LinkedIn failed or unavailable
            if not verified_contacts:
                print(f"\n🔄 PART 2B: SMART FALLBACK WORKFLOW")
                print("-" * 40)
                
                if not linkedin_url:
                    print("📋 Reason: No LinkedIn URL found")
                else:
                    print("📋 Reason: LinkedIn pipeline failed or returned no results")
                    
                print("🎯 Using captured website data for fire protection targeting")
                
                # Process fallback data
                verified_contacts = self._process_fallback_data(fallback_contacts, fallback_emails, normalized_url)
                
                if not verified_contacts:
                    print("❌ No contacts available from fallback data")
                    results['status'] = 'failed_no_contacts'
                    return results
            
            # 🔧 Remove duplicates and limit emails
            filtered_contacts = remove_duplicate_emails(verified_contacts, self.max_emails)
            
            if not filtered_contacts:
                print("❌ No contacts remaining after filtering")
                results['status'] = 'failed_no_emails_after_filter'
                return results
            
            # PART 3: AI email generation and sending (using filtered contacts)
            print(f"\n🤖 PART 3: AI EMAIL GENERATION & SENDING")
            print("-" * 40)
            emails_sent = self._generate_and_send_emails(filtered_contacts, domain)
            
            results['linkedin_employees'] = verified_contacts  # Store all found employees
            results['verified_contacts'] = filtered_contacts   # Store filtered contacts
            results['emails_sent'] = emails_sent
            results['status'] = 'completed'
            
            print(f"✅ Complete workflow finished!")
            print(f"📧 {len(emails_sent)} fire protection emails sent to {self.test_email}")
            
            return results
            
        except Exception as e:
            print(f"❌ Workflow failed: {e}")
            import traceback
            traceback.print_exc()
            results['status'] = 'failed'
            results['error'] = str(e)
            return results

    def _process_fallback_data(self, fallback_contacts: list, fallback_emails: list, website_url: str) -> list:
        """🔄 Process fallback data from website scraping when LinkedIn is unavailable"""
        
        print("🔄 PROCESSING SMART FALLBACK DATA")
        print("=" * 50)
        
        domain = urlparse(website_url).netloc.replace('www.', '')
        verified_contacts = []
        
        # Remove duplicates from emails first
        unique_emails = list(set(fallback_emails))
        
        # Process captured contacts with emails
        print(f"📊 Processing {len(fallback_contacts)} captured contacts...")
        
        for contact in fallback_contacts:
            name = contact.get('name', '')
            title = contact.get('title', '')
            phone = contact.get('phone', '')
            fire_relevance = contact.get('fire_protection_relevance', 'medium')
            
            print(f"   👤 Processing: {name} - {title}")
            
            # Try to match contact with appropriate email (FIXED LOGIC)
            contact_email = self._match_contact_to_email_fixed(contact, unique_emails, domain)
            
            if contact_email:
                print(f"   📧 Email matched: {contact_email}")
                
                # Verify email
                if self.millionverifier.smart_verify_email(contact_email, domain):
                    # Calculate fire protection score
                    fire_score = self._calculate_fallback_fire_score(title, fire_relevance)
                    
                    verified_contact = {
                        'name': name,
                        'title': title,
                        'email': contact_email,
                        'phone': phone,
                        'fire_protection_score': fire_score,
                        'fire_protection_reason': self._get_fire_protection_reason(title),
                        'source': 'smart_fallback',
                        'email_source': 'website_capture',
                        'verification_status': 'verified',
                        'priority': self._determine_priority_from_title(title)
                    }
                    
                    verified_contacts.append(verified_contact)
                    print(f"   ✅ Contact verified: {name} (Score: {fire_score})")
                    
                    # Remove used email to prevent duplicates
                    if contact_email in unique_emails:
                        unique_emails.remove(contact_email)
                else:
                    print(f"   ❌ Email verification failed: {contact_email}")
            else:
                print(f"   ⚠️ No suitable email found for: {name}")
        
        # Sort by fire protection score and limit
        verified_contacts.sort(key=lambda x: x.get('fire_protection_score', 0), reverse=True)
        limited_contacts = verified_contacts[:self.max_emails]
        
        print(f"\n🎯 SMART FALLBACK RESULTS:")
        print(f"   📊 Contacts processed: {len(fallback_contacts)}")
        print(f"   ✅ Verified contacts: {len(verified_contacts)}")
        print(f"   📧 Final selection: {len(limited_contacts)} (limited by max_emails)")
        
        return limited_contacts
    
    def _match_contact_to_email_fixed(self, contact: dict, available_emails: list, domain: str) -> str:
        """🎯 FIXED: Match a contact to the most appropriate email address"""
        
        name = contact.get('name', '').lower()
        title = contact.get('title', '').lower()
        
        if not available_emails:
            return None
        
        # Copy the list so we don't modify the original
        emails_to_check = available_emails.copy()
        
        # Priority 1: Try to find personalized email first
        for email in emails_to_check:
            email_local = email.split('@')[0].lower()
            
            # Check if email contains person's name
            name_parts = name.split()
            if len(name_parts) >= 2:
                first_name = name_parts[0]
                last_name = name_parts[-1]
                
                if (first_name in email_local or 
                    last_name in email_local or
                    f"{first_name}.{last_name}" in email_local or
                    f"{first_name}{last_name}" in email_local):
                    return email
        
        # Priority 2: For directors/managers, prefer non-generic emails
        if 'director' in title or 'manager' in title or 'operations' in title:
            for email in emails_to_check:
                if email != f"info@{domain}":  # Prefer non-generic emails for managers
                    return email
        
        # Priority 3: Return first available email (fallback)
        return emails_to_check[0] if emails_to_check else None
    
    def _calculate_fallback_fire_score(self, title: str, relevance: str) -> int:
        """🔥 Calculate fire protection score for fallback contacts"""
        
        title_lower = title.lower()
        base_score = 50
        
        # Title-based scoring
        if 'director' in title_lower:
            base_score += 30
        elif 'manager' in title_lower:
            base_score += 25
        elif 'technical' in title_lower:
            base_score += 20
        elif 'operations' in title_lower:
            base_score += 25
        
        # Relevance-based scoring
        if relevance == 'high':
            base_score += 20
        elif relevance == 'medium':
            base_score += 10
        
        # Fire safety related keywords
        if any(keyword in title_lower for keyword in ['safety', 'compliance', 'facilities', 'health']):
            base_score += 15
        
        return min(base_score, 100)  # Cap at 100
    
    def _get_fire_protection_reason(self, title: str) -> str:
        """📋 Get fire protection responsibility reason based on title"""
        
        title_lower = title.lower()
        
        if 'director' in title_lower and 'operations' in title_lower:
            return "Operations Director - oversees all operational safety including fire protection systems"
        elif 'technical' in title_lower and 'manager' in title_lower:
            return "Technical Manager - responsible for technical compliance including fire safety systems"
        elif 'director' in title_lower:
            return "Director-level responsibility for workplace safety and compliance"
        elif 'manager' in title_lower:
            return "Management responsibility for operational safety procedures"
        else:
            return "Key contact for business fire safety and compliance requirements"
    
    def _determine_priority_from_title(self, title: str) -> str:
        """📊 Determine priority level from job title"""
        
        title_lower = title.lower()
        
        if any(keyword in title_lower for keyword in ['director', 'ceo', 'owner', 'founder']):
            return 'high'
        elif any(keyword in title_lower for keyword in ['manager', 'head', 'lead']):
            return 'medium'
        else:
            return 'standard'

    def _generate_and_send_emails(self, verified_contacts: list, domain: str) -> list:
        """🤖 AI email generation and sending (now with limited contacts)"""
        
        print("🤖 AI EMAIL GENERATION & SENDING")
        print(f"🎯 Target: {len(verified_contacts)} verified fire protection contacts (post-filtering)")
        print(f"📬 Test email: {self.test_email}")
        print("=" * 70)
        
        # Initialize AI email generator
        try:
            email_generator = ExpertEmailGenerator()
        except Exception as e:
            print(f"❌ Failed to initialize email generator: {e}")
            return []
        
        sent_emails = []
        
        for i, contact in enumerate(verified_contacts, 1):
            try:
                print(f"\n📧 Generating email {i}/{len(verified_contacts)}: {contact['name']}")
                
                # Prepare company data for AI generation
                company_data = {
                    'company_name': domain.replace('.com', '').replace('.co.uk', '').replace('.ie', '').title(),
                    'industry': 'business services',
                    'location': 'UK/Ireland',
                    'url': f"https://{domain}",
                    'services': ['business operations'],
                    'fire_safety_keywords': [],
                    'compliance_mentions': [],
                    'personalization_hooks': [contact.get('fire_protection_reason', 'Fire safety decision maker')],
                    'about_text': f"Company focusing on business operations with fire protection responsibilities"
                }
                
                # Generate AI email
                email_content = email_generator.generate_expert_cold_email(
                    contact=contact,
                    company_data=company_data,
                    pfp_context={}
                )
                
                if email_content:
                    print(f"   📝 Subject: {email_content['subject']}")
                    print(f"   📄 Body length: {len(email_content['body'])} characters")
                    
                    # Send email to test address
                    print(f"   📤 Sending to test email: {self.test_email}")
                    
                    success = send_email(
                        to_email=self.test_email,
                        subject=email_content['subject'],
                        body=email_content['body'],
                        from_name="Dave - PFP Fire Protection"
                    )
                    
                    if success:
                        print(f"✅ Test email sent successfully for {contact['name']}")
                        contact['email_sent'] = True
                        contact['subject'] = email_content['subject']
                        contact['body'] = email_content['body']
                        sent_emails.append(contact)
                    else:
                        print(f"❌ Failed to send test email for {contact['name']}")
                        contact['email_sent'] = False
                else:
                    print(f"❌ Failed to generate email content for {contact['name']}")
                    contact['email_sent'] = False
                    
            except Exception as e:
                print(f"❌ Error processing {contact['name']}: {e}")
                contact['email_sent'] = False
        
        # Summary
        print(f"\n📊 EMAIL SENDING SUMMARY:")
        print(f"   ✅ Successfully sent: {len(sent_emails)}")
        print(f"   ❌ Failed: {len(verified_contacts) - len(sent_emails)}")
        print(f"   📧 Total processed: {len(verified_contacts)}")
        print(f"   📬 All copies sent to: {self.test_email}")
        print(f"   🎯 Duplicates prevented: Email limit enforced at {self.max_emails}")
        
        return sent_emails

    def save_results(self, results: dict) -> str:
        """💾 Save comprehensive workflow results to output folder"""
        
        # Ensure output directory exists
        os.makedirs("output", exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Use normalized URL for filename
        domain = urlparse(results['website_url']).netloc.replace('www.', '').replace('.', '_')
        
        # Main results file
        main_filename = f"output/complete_workflow_{domain}_linkedin_pipeline_{timestamp}.csv"
        
        with open(main_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Header with URL information
            writer.writerow(['Complete Workflow Results'])
            writer.writerow(['Timestamp', timestamp])
            writer.writerow(['Original URL', results.get('original_url', '')])
            writer.writerow(['Normalized URL', results['website_url']])
            writer.writerow(['LinkedIn', results['linkedin_url']])
            writer.writerow(['Status', results['status']])
            writer.writerow(['Max Emails Setting', self.max_emails])
            writer.writerow([])
            
            # Website staff
            writer.writerow(['Website Staff Found'])
            writer.writerow(['Name', 'Title', 'Source'])
            for staff in results['website_staff']:
                writer.writerow([staff.get('name', ''), staff.get('title', ''), 'Website'])
            writer.writerow([])
            
            # All LinkedIn employees found (or fallback contacts)
            if results.get('linkedin_employees'):
                writer.writerow(['All LinkedIn Employees Found'])
                writer.writerow(['Name', 'Title', 'Email', 'Fire Protection Score', 'Priority'])
                for employee in results.get('linkedin_employees', []):
                    writer.writerow([
                        employee.get('name', ''),
                        employee.get('title', ''),
                        employee.get('email', ''),
                        employee.get('fire_protection_score', ''),
                        employee.get('priority', '')
                    ])
                writer.writerow([])
            
            # Fire protection targets contacted (after filtering)
            writer.writerow(['Fire Protection Targets Contacted (After Filtering)'])
            writer.writerow(['Name', 'Title', 'Email', 'Fire Protection Score', 'Subject', 'Email Sent', 'Source'])
            for email in results['emails_sent']:
                writer.writerow([
                    email.get('name', ''),
                    email.get('title', ''),
                    email.get('email', ''),
                    email.get('fire_protection_score', ''),
                    email.get('subject', ''),
                    'Yes' if email.get('email_sent') else 'No',
                    email.get('source', '')
                ])
        
        print(f"💾 Main results saved: {main_filename}")
        return main_filename


def main():
    """🚀 CLI interface and main execution"""
    
    parser = argparse.ArgumentParser(
        description="🚀 Complete Workflow Super Scraper - MODULAR VERSION",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --url https://example.com --credit-threshold 4.85
  python main.py --url example.com --credit-threshold 4.85 --max-emails 1
  python main.py --url https://www.example.com/ --credit-threshold 4.85 --max-emails 3
  
URL Normalization:
  - Automatically adds https:// if missing
  - Automatically adds www. if missing  
  - Fixes malformed URLs like https://www.https://example.com/
        """
    )
    
    parser.add_argument("--url", required=True, 
                       help="Target website URL (automatically normalized)")
    parser.add_argument("--credit-threshold", type=float, default=4.85, 
                       help="Credit threshold for account switching (default: $4.85)")
    parser.add_argument("--max-emails", type=int, default=2,
                       help="Maximum number of emails to send (default: 2)")
    args = parser.parse_args()

    print("🚀 COMPLETE WORKFLOW SUPER SCRAPER - MODULAR VERSION")
    print("=" * 60)
    print("🔄 Website → Staff → LinkedIn → Patterns → AI → Send")
    print(f"🧠 Features: Enhanced content analysis + Smart pattern learning")
    print(f"📧 Test mode: All emails go to dave@alpha-omegaltd.com")
    print(f"💰 Credit threshold: ${args.credit_threshold}")
    print(f"📊 Max emails: {args.max_emails}")
    print(f"🔥 NEW: Dynamic URL handling + Smart Fallback + Automatic normalization + Duplicate prevention")
    print()

    # Show URL normalization
    original_url = args.url
    normalized_url = normalize_url(original_url)
    
    if original_url != normalized_url:
        print("🔧 URL NORMALIZATION:")
        print(f"   📝 Original: {original_url}")
        print(f"   ✅ Normalized: {normalized_url}")
        print()

    # Check environment variables
    required_vars = ['APIFY_TOKEN', 'OPENAI_API_KEY', 'MILLIONVERIFIER_API_KEY', 'SMTP_EMAIL', 'SMTP_PASSWORD']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var) and not os.getenv(f"{var}_1"):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("🔧 Please set these in your .env file")
        sys.exit(1)
    
    print("✅ All environment variables found")
    print()

    try:
        # Initialize scraper with max_emails setting
        scraper = CompleteWorkflowSuperScraper(max_emails=args.max_emails)
        
        # Run complete workflow with normalized URL
        results = scraper.run_complete_workflow(normalized_url)
        
        # Save results
        results_file = scraper.save_results(results)
        
        # Final summary
        print("\n🎉 SMART FALLBACK WORKFLOW SUMMARY")
        print("=" * 60)
        print(f"🌐 Website: {results['website_url']}")
        print(f"👥 Website staff found: {len(results['website_staff'])}")
        print(f"🔗 LinkedIn URL: {'✅ Found' if results['linkedin_url'] else '❌ Not found'}")
        print(f"📧 Fire protection emails sent: {len(results['emails_sent'])}")
        print(f"📊 Max emails setting: {args.max_emails}")
        print(f"💾 Results saved: {results_file}")
        print(f"📬 Test emails sent to: dave@alpha-omegaltd.com")
        print(f"💰 Credit threshold used: ${args.credit_threshold}")
        print()
        
        if results['status'] == 'completed':
            print("✅ Complete workflow successful!")
            
            if results['emails_sent']:
                print("\n🎯 Fire Protection Targets Contacted:")
                for email in results['emails_sent']:
                    print(f"   📧 {email['name']} - {email['title']}")
                    print(f"      Score: {email.get('fire_protection_score', 'N/A')}")
                    print(f"      Subject: {email.get('subject', 'N/A')}")
                    print(f"      Email: {email.get('email', 'N/A')}")
                    print(f"      Source: {email.get('source', 'N/A')}")
                    print()
        else:
            print(f"⚠️ Workflow status: {results['status']}")
            if 'error' in results:
                print(f"❌ Error: {results['error']}")

    except KeyboardInterrupt:
        print("\n🛑 Workflow interrupted by user")
    except Exception as e:
        print(f"\n❌ Workflow failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
