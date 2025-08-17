#!/usr/bin/env python3
"""
🚀 COMPLETE WORKFLOW SUPER SCRAPER - BULLETPROOF VERSION
=======================================================
✅ FIXED: All syntax errors and duplicates removed
✅ FIXED: Modular structure for easier maintenance
✅ FIXED: Enhanced staff extraction with increased content limits
✅ FIXED: Smart pattern learning and email discovery
✅ FIXED: Real-time credit monitoring and account rotation
✅ NEW: Bulletproof fallback system for websites without LinkedIn

Usage: python main.py --url https://www.vikingstaffingandevents.co.uk/

Features:
✅ Website scraping with enhanced GPT-4o analysis (100K chars vs 15K)
✅ LinkedIn employee discovery with smart email patterns
✅ 🛡️ BULLETPROOF FALLBACK: Works even without LinkedIn
✅ Direct email extraction from website content
✅ GPT-4o-mini contact discovery from website content
✅ Smart MillionVerifier with catch-all domain intelligence
✅ Pattern learning and email generation for discovered contacts
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
    """🚀 Complete workflow orchestrator with bulletproof fallback systems"""
    
    def __init__(self):
        self.openai_key = os.getenv('OPENAI_API_KEY')
        self.test_email = "dave@alpha-omegaltd.com"
        
        if not self.openai_key:
            raise RuntimeError("🚨 Missing API keys: OPENAI_API_KEY required")
        
        # Initialize managers
        self.millionverifier = MillionVerifierManager()
        self.website_scraper = WebsiteScraper(self.openai_key)
        self.linkedin_scraper = LinkedInScraper(self.openai_key, self.millionverifier)
        
        print("🚀 COMPLETE WORKFLOW SUPER SCRAPER INITIALIZED - BULLETPROOF VERSION")
        print(f"🧠 AI: GPT-4o analysis + GPT-4o-mini person validation")
        print(f"🛡️ Stealth: Apify cheapest proxies with account rotation")
        print(f"🛡️ NEW: Bulletproof fallback for websites without LinkedIn")
        print(f"📧 Test emails: {self.test_email}")

    def run_complete_workflow(self, website_url: str) -> dict:
        """🎯 BULLETPROOF workflow: Website → LinkedIn OR Website Fallback → Emails → Send"""
        
        print("\n🎯 STARTING BULLETPROOF COMPLETE WORKFLOW")
        print("=" * 70)
        print(f"🌐 Target: {website_url}")
        print(f"🔄 Process: Website → Staff → LinkedIn OR Fallback → Patterns → AI → Send")
        print(f"🛡️ Fallback: Direct website contact extraction if LinkedIn fails")
        print(f"📧 Test mode: All emails go to {self.test_email}")
        print()
        
        results = {
            'website_url': website_url,
            'website_staff': [],
            'linkedin_url': '',
            'linkedin_employees': [],
            'fallback_contacts': [],
            'verified_contacts': [],
            'emails_sent': [],
            'status': 'started',
            'workflow_path': 'unknown'
        }
        
        try:
            # PART 1: Website scraping
            print("📋 PART 1: WEBSITE SCRAPING")
            print("-" * 40)
            website_staff, linkedin_url = self.website_scraper.scrape_website_for_staff_and_linkedin(website_url)
            
            results['website_staff'] = website_staff
            results['linkedin_url'] = linkedin_url
            
            print(f"✅ Part 1 complete: {len(website_staff)} staff + LinkedIn URL")
            
            # Cooling-off period before Part 2
            print("\n⏳ COOLING-OFF PERIOD: Waiting 3 seconds before next phase...")
            time.sleep(3)
            
            # DECISION POINT: LinkedIn or Fallback?
            if linkedin_url:
                print(f"\n🔗 PART 2: LINKEDIN PIPELINE")
                print("-" * 40)
                print(f"🏢 LinkedIn URL found: {linkedin_url}")
                print(f"✅ Using LinkedIn pipeline with advanced features")
                
                results['workflow_path'] = 'linkedin_pipeline'
                
                # Extract domain for LinkedIn processing
                domain = urlparse(website_url).netloc.replace('www.', '')
                verified_contacts = self.linkedin_scraper.scrape_linkedin_and_discover_emails(linkedin_url, domain)
                
                if verified_contacts:
                    results['linkedin_employees'] = verified_contacts
                    results['verified_contacts'] = verified_contacts
                else:
                    print("⚠️ LinkedIn pipeline found no contacts - trying smart fallback...")
                    verified_contacts = self._run_smart_fallback_pipeline(results)
                    
            else:
                print(f"\n🛡️ PART 2: SMART FALLBACK PIPELINE")
                print("-" * 50)
                print(f"❌ No LinkedIn URL found - using pre-captured fallback data")
                print(f"🎯 Strategy: Use contact data already captured in Part 1")
                
                results['workflow_path'] = 'smart_fallback_pipeline'
                verified_contacts = self._run_smart_fallback_pipeline(results)
            
            if not verified_contacts:
                print("❌ No verified email addresses found in any pipeline")
                results['status'] = 'failed_no_contacts'
                return results
            
            # PART 3: AI email generation and sending
            print(f"\n🤖 PART 3: AI EMAIL GENERATION & SENDING")
            print("-" * 40)
            domain = urlparse(website_url).netloc.replace('www.', '')
            emails_sent = self._generate_and_send_emails(verified_contacts, domain)
            
            results['emails_sent'] = emails_sent
            results['status'] = 'completed'
            
            print(f"✅ Complete bulletproof workflow finished!")
            print(f"📧 {len(emails_sent)} fire protection emails sent to {self.test_email}")
            print(f"🔄 Workflow path used: {results['workflow_path']}")
            
            return results
            
        except Exception as e:
            print(f"❌ Workflow failed: {e}")
            results['status'] = 'failed'
            results['error'] = str(e)
            return results

    def _run_smart_fallback_pipeline(self, results: dict) -> list:
        """🛡️ Run the smart fallback pipeline using pre-captured data"""
        
        # Use the pre-captured data from Part 1 - no re-scraping!
        fallback_contacts = self.website_scraper.get_fallback_contacts()
        results['fallback_contacts'] = fallback_contacts
        
        if fallback_contacts:
            print(f"🎉 Smart fallback successful: {len(fallback_contacts)} contacts from pre-captured data")
            
            # Take top contacts based on fire protection score
            top_contacts = sorted(fallback_contacts, 
                                key=lambda x: x.get('fire_protection_score', 0), 
                                reverse=True)[:3]  # Top 3 contacts
            
            print(f"🎯 Selected top {len(top_contacts)} contacts for email generation:")
            for i, contact in enumerate(top_contacts, 1):
                name = contact.get('name', 'Unknown')
                score = contact.get('fire_protection_score', 0)
                email = contact.get('email', 'No email')
                source = contact.get('email_source', 'unknown')
                print(f"   {i}. {name} - Score: {score} - Email: {email} - Source: {source}")
            
            return top_contacts
        else:
            print("❌ Smart fallback pipeline found no contacts in pre-captured data")
            return []

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
        workflow_path = results.get('workflow_path', 'unknown')
        
        # Main results file
        main_filename = f"output/complete_workflow_{domain}_{workflow_path}_{timestamp}.csv"
        
        with open(main_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow(['Complete Bulletproof Workflow Results'])
            writer.writerow(['Timestamp', timestamp])
            writer.writerow(['Website', results['website_url']])
            writer.writerow(['LinkedIn', results['linkedin_url']])
            writer.writerow(['Workflow Path', workflow_path])
            writer.writerow(['Status', results['status']])
            writer.writerow([])
            
            # Website staff
            writer.writerow(['Website Staff Found'])
            writer.writerow(['Name', 'Title', 'Source'])
            for staff in results['website_staff']:
                writer.writerow([staff.get('name', ''), staff.get('title', ''), 'Website'])
            writer.writerow([])
            
            # LinkedIn employees (if any)
            if results.get('linkedin_employees'):
                writer.writerow(['LinkedIn Employees Found'])
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
            
            # Fallback contacts (if any)
            if results.get('fallback_contacts'):
                writer.writerow(['Website Fallback Contacts Found'])
                writer.writerow(['Name', 'Title', 'Email', 'Fire Protection Score', 'Email Source', 'Role Type'])
                for contact in results.get('fallback_contacts', []):
                    writer.writerow([
                        contact.get('name', ''),
                        contact.get('title', ''),
                        contact.get('email', ''),
                        contact.get('fire_protection_score', ''),
                        contact.get('email_source', ''),
                        contact.get('role_type', '')
                    ])
                writer.writerow([])
            
            # Final contacted targets
            writer.writerow(['Fire Protection Targets Contacted'])
            writer.writerow(['Name', 'Title', 'Email', 'Fire Protection Score', 'Subject', 'Email Sent', 'Contact Source'])
            for email in results['emails_sent']:
                writer.writerow([
                    email.get('name', ''),
                    email.get('title', ''),
                    email.get('email', ''),
                    email.get('fire_protection_score', ''),
                    email.get('subject', ''),
                    'Yes' if email.get('email_sent') else 'No',
                    email.get('email_source', '')
                ])
        
        print(f"💾 Results saved: {main_filename}")
        
        return main_filename


def main():
    """🚀 CLI interface and main execution"""
    
    parser = argparse.ArgumentParser(description="🚀 Complete Workflow Super Scraper - BULLETPROOF VERSION")
    parser.add_argument("--url", required=True, help="Target website URL")
    parser.add_argument("--credit-threshold", type=float, default=4.85, 
                       help="Credit threshold for account switching (default: $4.85)")
    args = parser.parse_args()

    print("🚀 COMPLETE WORKFLOW SUPER SCRAPER - SMART FALLBACK VERSION")
    print("=" * 70)
    print("🔄 Website → Staff → LinkedIn OR Smart Fallback → Patterns → AI → Send")
    print(f"🧠 Features: Enhanced content analysis + Smart pattern learning")
    print(f"🛡️ SMART: Uses pre-captured data - no re-scraping needed!")
    print(f"📧 Test mode: All emails go to dave@alpha-omegaltd.com")
    print(f"💰 Credit threshold: ${args.credit_threshold}")
    print(f"🔥 EFFICIENT: Captures all data once in Part 1!")
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
        print("\n🎉 SMART FALLBACK WORKFLOW SUMMARY")
        print("=" * 60)
        print(f"🌐 Website: {results['website_url']}")
        print(f"👥 Website staff found: {len(results['website_staff'])}")
        print(f"🔗 LinkedIn URL: {'✅ Found' if results['linkedin_url'] else '❌ Not found'}")
        print(f"🛡️ Workflow path: {results.get('workflow_path', 'unknown').upper()}")
        
        if results.get('linkedin_employees'):
            print(f"🔗 LinkedIn employees: {len(results['linkedin_employees'])}")
        if results.get('fallback_contacts'):
            print(f"🛡️ Fallback contacts: {len(results['fallback_contacts'])}")
            
        print(f"📧 Fire protection emails sent: {len(results['emails_sent'])}")
        print(f"💾 Results saved: {results_file}")
        print(f"📬 Test emails sent to: dave@alpha-omegaltd.com")
        print(f"💰 Credit threshold used: ${args.credit_threshold}")
        print()
        
        if results['status'] == 'completed':
            print("✅ Smart fallback workflow successful!")
            
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
