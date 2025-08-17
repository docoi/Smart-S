#!/usr/bin/env python3
"""
🚀 COMPLETE WORKFLOW SUPER SCRAPER - MODULAR VERSION
==================================================
✅ FIXED: All syntax errors and duplicates removed
✅ FIXED: Modular structure for easier maintenance
✅ FIXED: Enhanced staff extraction with increased content limits
✅ FIXED: Smart pattern learning and email discovery
✅ FIXED: Real-time credit monitoring and account rotation

Usage: python main.py --url https://www.vikingstaffingandevents.co.uk/

Features:
✅ Website scraping with enhanced GPT-4o analysis (100K chars vs 15K)
✅ LinkedIn employee discovery with smart email patterns
✅ Real-time credit monitoring with automatic account switching
✅ Smart MillionVerifier with catch-all domain intelligence
✅ GPT-4o-mini person validation (filters out company accounts)
✅ Pattern learning that discovers successful patterns and applies to all contacts
✅ Fire protection targeting with advanced scoring
✅ AI email generation and sending
"""

from __future__ import annotations
import os
import sys
import csv
import time
import argparse
from datetime import datetime
from urllib.parse import urlparse
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


class CompleteWorkflowSuperScraper:
    """🚀 Complete workflow orchestrator - coordinates all modules"""
    
    def __init__(self):
        self.openai_key = os.getenv('OPENAI_API_KEY')
        self.test_email = "dave@alpha-omegaltd.com"
        
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

    def run_complete_workflow(self, website_url: str) -> dict:
        """🎯 Complete workflow: Website → LinkedIn → Emails → Send"""
        
        print("\n🎯 STARTING COMPLETE WORKFLOW")
        print("=" * 60)
        print(f"🌐 Target: {website_url}")
        print(f"🔄 Process: Website → Staff → LinkedIn → Patterns → AI → Send")
        print(f"📧 Test mode: All emails go to {self.test_email}")
        print()
        
        results = {
            'website_url': website_url,
            'website_staff': [],
            'linkedin_url': '',
            'linkedin_employees': [],
            'verified_contacts': [],
            'emails_sent': [],
            'status': 'started'
        }
        
        try:
            # PART 1: Website scraping
            print("📋 PART 1: WEBSITE SCRAPING")
            print("-" * 40)
            website_staff, linkedin_url = self.website_scraper.scrape_website_for_staff_and_linkedin(website_url)
            
            results['website_staff'] = website_staff
            results['linkedin_url'] = linkedin_url
            
            if not linkedin_url:
                print("❌ No LinkedIn URL found - cannot continue to Part 2")
                results['status'] = 'failed_no_linkedin'
                return results
            
            print(f"✅ Part 1 complete: {len(website_staff)} staff + LinkedIn URL")
            
            # Cooling-off period before Part 2
            print("\n⏳ COOLING-OFF PERIOD: Waiting 3 seconds before LinkedIn phase...")
            time.sleep(3)
            
            # PART 2: LinkedIn pipeline
            print(f"\n🔗 PART 2: LINKEDIN PIPELINE")
            print("-" * 40)
            print(f"🏢 Using modular LinkedIn pipeline with advanced features")
            print(f"✅ Features: Actor 2 + 33 Golden Patterns + Smart MillionVerifier + Pattern Learning")
            
            # Extract domain for LinkedIn processing
            domain = urlparse(website_url).netloc.replace('www.', '')
            verified_contacts = self.linkedin_scraper.scrape_linkedin_and_discover_emails(linkedin_url, domain)
            
            if not verified_contacts:
                print("❌ No verified email addresses found")
                results['status'] = 'failed_no_emails'
                return results
            
            # PART 3: AI email generation and sending
            print(f"\n🤖 PART 3: AI EMAIL GENERATION & SENDING")
            print("-" * 40)
            emails_sent = self._generate_and_send_emails(verified_contacts, domain)
            
            results['linkedin_employees'] = verified_contacts  # Store all found employees
            results['verified_contacts'] = verified_contacts
            results['emails_sent'] = emails_sent
            results['status'] = 'completed'
            
            print(f"✅ Complete workflow finished!")
            print(f"📧 {len(emails_sent)} fire protection emails sent to {self.test_email}")
            
            return results
            
        except Exception as e:
            print(f"❌ Workflow failed: {e}")
            results['status'] = 'failed'
            results['error'] = str(e)
            return results

    def _generate_and_send_emails(self, verified_contacts: list, domain: str) -> list:
        """🤖 AI email generation and sending"""
        
        print("🤖 AI EMAIL GENERATION & SENDING")
        print(f"🎯 Target: {len(verified_contacts)} verified fire protection contacts")
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
        
        return sent_emails

    def save_results(self, results: dict) -> str:
        """💾 Save comprehensive workflow results to output folder"""
        
        # Ensure output directory exists
        os.makedirs("output", exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain = urlparse(results['website_url']).netloc.replace('www.', '').replace('.', '_')
        
        # Main results file
        main_filename = f"output/complete_workflow_{domain}_{timestamp}.csv"
        
        with open(main_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow(['Complete Workflow Results'])
            writer.writerow(['Timestamp', timestamp])
            writer.writerow(['Website', results['website_url']])
            writer.writerow(['LinkedIn', results['linkedin_url']])
            writer.writerow(['Status', results['status']])
            writer.writerow([])
            
            # Website staff
            writer.writerow(['Website Staff Found'])
            writer.writerow(['Name', 'Title', 'Source'])
            for staff in results['website_staff']:
                writer.writerow([staff.get('name', ''), staff.get('title', ''), 'Website'])
            writer.writerow([])
            
            # All LinkedIn employees found
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
            
            # Fire protection targets contacted
            writer.writerow(['Fire Protection Targets Contacted'])
            writer.writerow(['Name', 'Title', 'Email', 'Fire Protection Score', 'Subject', 'Email Sent'])
            for email in results['emails_sent']:
                writer.writerow([
                    email.get('name', ''),
                    email.get('title', ''),
                    email.get('email', ''),
                    email.get('fire_protection_score', ''),
                    email.get('subject', ''),
                    'Yes' if email.get('email_sent') else 'No'
                ])
        
        # Separate detailed staff file
        staff_filename = f"output/all_staff_{domain}_{timestamp}.csv"
        
        with open(staff_filename, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['name', 'title', 'email', 'source', 'fire_protection_score', 'fire_protection_reason', 'priority', 'company', 'linkedin_profile_url']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            # Add website staff
            for staff in results['website_staff']:
                writer.writerow({
                    'name': staff.get('name', ''),
                    'title': staff.get('title', ''),
                    'email': '',
                    'source': 'Website',
                    'fire_protection_score': '',
                    'fire_protection_reason': '',
                    'priority': '',
                    'company': '',
                    'linkedin_profile_url': ''
                })
            
            # Add all LinkedIn employees
            for employee in results.get('linkedin_employees', []):
                writer.writerow({
                    'name': employee.get('name', ''),
                    'title': employee.get('title', ''),
                    'email': employee.get('email', ''),
                    'source': 'LinkedIn',
                    'fire_protection_score': employee.get('fire_protection_score', ''),
                    'fire_protection_reason': employee.get('fire_protection_reason', ''),
                    'priority': employee.get('priority', ''),
                    'company': employee.get('company', ''),
                    'linkedin_profile_url': employee.get('linkedin_profile_url', '')
                })
        
        print(f"💾 Main results saved: {main_filename}")
        print(f"💾 Staff details saved: {staff_filename}")
        
        return main_filename


def main():
    """🚀 CLI interface and main execution"""
    
    parser = argparse.ArgumentParser(description="🚀 Complete Workflow Super Scraper - MODULAR VERSION")
    parser.add_argument("--url", required=True, help="Target website URL")
    parser.add_argument("--credit-threshold", type=float, default=4.85, 
                       help="Credit threshold for account switching (default: $4.85)")
    args = parser.parse_args()

    print("🚀 COMPLETE WORKFLOW SUPER SCRAPER - MODULAR VERSION")
    print("=" * 60)
    print("🔄 Website → Staff → LinkedIn → Patterns → AI → Send")
    print(f"🧠 Features: Enhanced content analysis + Smart pattern learning")
    print(f"📧 Test mode: All emails go to dave@alpha-omegaltd.com")
    print(f"💰 Credit threshold: ${args.credit_threshold}")
    print(f"🔥 NEW: Modular structure + Enhanced staff extraction")
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
        # Initialize scraper
        scraper = CompleteWorkflowSuperScraper()
        
        # Run complete workflow
        results = scraper.run_complete_workflow(args.url)
        
        # Save results
        results_file = scraper.save_results(results)
        
        # Final summary
        print("\n🎉 COMPLETE WORKFLOW SUMMARY")
        print("=" * 50)
        print(f"🌐 Website: {results['website_url']}")
        print(f"👥 Website staff found: {len(results['website_staff'])}")
        print(f"🔗 LinkedIn URL: {'✅ Found' if results['linkedin_url'] else '❌ Not found'}")
        print(f"📧 Fire protection emails sent: {len(results['emails_sent'])}")
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
                    print(f"      Source: {email.get('email_source', 'N/A')}")
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