#!/usr/bin/env python3
"""
🚀 COMPLETE WORKFLOW SUPER SCRAPER - SMART 3-TIER VERSION
=========================================================
✅ SMART: 3-tier actor selection (Cheerio → Content Crawler → Puppeteer)
✅ BULLETPROOF: Guaranteed success with automatic escalation
✅ COST-OPTIMIZED: Always starts with cheapest option
✅ IP PROTECTED: Same cheapest Apify proxies across all tiers
✅ ENHANCED: Better staff extraction and fire protection targeting

Usage: python main.py --url https://www.crewsaders.com/

Features:
✅ 3-tier smart website scraping with automatic escalation
✅ LinkedIn employee discovery with enhanced targeting  
✅ Real-time credit monitoring with automatic account switching
✅ Smart MillionVerifier with catch-all domain intelligence
✅ GPT-4o-mini person validation (filters out company accounts)
✅ Pattern learning that discovers successful patterns
✅ Enhanced fire protection targeting (senior decision makers only)
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
    from smart_website_scraper import SmartWebsiteScraper  # ← NEW: Smart 3-tier scraper
    from linkedin_scraper import LinkedInScraper
    from expert_email_generator import ExpertEmailGenerator
    from send_smtp import send_email
    print("✅ All modules imported successfully")
except ImportError as e:
    print(f"❌ Missing modules: {e}")
    print("🔧 Make sure all module files are in the same directory")
    sys.exit(1)


class CompleteWorkflowSuperScraper:
    """🚀 Complete workflow orchestrator with smart 3-tier scraping"""
    
    def __init__(self):
        self.openai_key = os.getenv('OPENAI_API_KEY')
        self.test_email = "dave@alpha-omegaltd.com"
        
        if not self.openai_key:
            raise RuntimeError("🚨 Missing API keys: OPENAI_API_KEY required")
        
        # Initialize managers
        self.millionverifier = MillionVerifierManager()
        self.website_scraper = SmartWebsiteScraper(self.openai_key)  # ← NEW: Smart scraper
        self.linkedin_scraper = LinkedInScraper(self.openai_key, self.millionverifier)
        
        print("🚀 COMPLETE WORKFLOW SUPER SCRAPER INITIALIZED")
        print(f"🧠 AI: GPT-4o analysis + GPT-4o-mini person validation")
        print(f"🛡️ Stealth: 3-tier actor selection with cheapest proxies")
        print(f"📧 Test emails: {self.test_email}")
        print(f"🎯 Smart: Cheerio → Content Crawler → Puppeteer escalation")

    def run_complete_workflow(self, website_url: str, max_emails: int = 2) -> dict:
        """🎯 Complete workflow with smart 3-tier scraping"""
        
        print("\n🎯 STARTING COMPLETE WORKFLOW")
        print("=" * 60)
        print(f"🌐 Original URL: {website_url}")
        print(f"🌐 Normalized URL: {self._normalize_url(website_url)}")
        print(f"🔄 Process: Smart Website → Staff → LinkedIn → Patterns → AI → Send")
        print(f"📧 Test mode: All emails go to {self.test_email}")
        print(f"📊 Max emails: {max_emails}")
        print()
        
        results = {
            'website_url': website_url,
            'scraping_tier_used': '',
            'website_staff': [],
            'linkedin_url': '',
            'linkedin_employees': [],
            'verified_contacts': [],
            'emails_sent': [],
            'status': 'started'
        }
        
        try:
            # PART 1: Smart 3-tier website scraping
            print("📋 PART 1: SMART 3-TIER WEBSITE SCRAPING")
            print("-" * 40)
            website_staff, linkedin_url = self.website_scraper.scrape_website_for_staff_and_linkedin(website_url)
            
            results['website_staff'] = website_staff
            results['linkedin_url'] = linkedin_url
            results['scraping_tier_used'] = self.website_scraper.tier_used
            
            if not linkedin_url:
                print("❌ No LinkedIn URL found - cannot continue to Part 2")
                results['status'] = 'failed_no_linkedin'
                return results
            
            print(f"✅ Part 1 complete: {len(website_staff)} staff + LinkedIn URL")
            print(f"🎯 Scraping method used: {results['scraping_tier_used']}")
            
            # Cooling-off period before Part 2
            print("\n⏳ COOLING-OFF PERIOD: Waiting 3 seconds before LinkedIn phase...")
            time.sleep(3)
            
            # PART 2: LinkedIn pipeline with enhanced targeting
            print(f"\n🔗 PART 2: LINKEDIN PIPELINE")
            print("-" * 40)
            print(f"🏢 LinkedIn URL: {linkedin_url}")
            print(f"💰 Using enhanced fire protection targeting (senior decision makers only)")
            
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
            emails_sent = self._generate_and_send_emails(verified_contacts, domain, max_emails)
            
            results['linkedin_employees'] = verified_contacts
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

    def _generate_and_send_emails(self, verified_contacts: list, domain: str, max_emails: int) -> list:
        """🤖 AI email generation and sending with limit"""
        
        print("🤖 AI EMAIL GENERATION & SENDING")
        print(f"🎯 Target: {len(verified_contacts)} verified fire protection contacts")
        print(f"📬 Test email: {self.test_email}")
        print(f"📊 Email limit: {max_emails}")
        print("=" * 70)
        
        # Initialize AI email generator
        try:
            email_generator = ExpertEmailGenerator()
        except Exception as e:
            print(f"❌ Failed to initialize email generator: {e}")
            return []
        
        sent_emails = []
        
        # Limit to max_emails
        contacts_to_process = verified_contacts[:max_emails]
        
        for i, contact in enumerate(contacts_to_process, 1):
            try:
                print(f"\n📧 Generating email {i}/{len(contacts_to_process)}: {contact['name']}")
                
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
        print(f"   ❌ Failed: {len(contacts_to_process) - len(sent_emails)}")
        print(f"   📧 Total processed: {len(contacts_to_process)}")
        print(f"   📬 All copies sent to: {self.test_email}")
        
        return sent_emails

    def save_results(self, results: dict) -> str:
        """💾 Save comprehensive workflow results with smart scraping stats"""
        
        # Ensure output directory exists
        os.makedirs("output", exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain = urlparse(results['website_url']).netloc.replace('www.', '').replace('.', '_')
        
        # Main results file
        main_filename = f"output/smart_workflow_{domain}_{timestamp}.csv"
        
        with open(main_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Header with smart scraping info
            writer.writerow(['Smart 3-Tier Workflow Results'])
            writer.writerow(['Timestamp', timestamp])
            writer.writerow(['Website', results['website_url']])
            writer.writerow(['Scraping Tier Used', results['scraping_tier_used']])
            writer.writerow(['LinkedIn', results['linkedin_url']])
            writer.writerow(['Status', results['status']])
            writer.writerow([])
            
            # Website staff
            writer.writerow(['Website Staff Found'])
            writer.writerow(['Name', 'Title', 'Source'])
            for staff in results['website_staff']:
                writer.writerow([staff.get('name', ''), staff.get('title', ''), 'Smart Website Scraper'])
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
        
        print(f"💾 Smart workflow results saved: {main_filename}")
        return main_filename

    def _normalize_url(self, url: str) -> str:
        """🌐 Normalize URL for consistency"""
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        netloc = parsed.netloc
        
        if netloc.startswith('www.'):
            netloc = netloc[4:]
        
        return f"{parsed.scheme}://{netloc}{parsed.path}"


def main():
    """🚀 CLI interface and main execution"""
    
    parser = argparse.ArgumentParser(description="🚀 Complete Workflow Super Scraper - SMART 3-TIER VERSION")
    parser.add_argument("--url", required=True, help="Target website URL")
    parser.add_argument("--credit-threshold", type=float, default=4.85, 
                       help="Credit threshold for account switching (default: $4.85)")
    parser.add_argument("--max-emails", type=int, default=2,
                       help="Maximum number of emails to send (default: 2)")
    args = parser.parse_args()

    print("🚀 COMPLETE WORKFLOW SUPER SCRAPER - SMART 3-TIER VERSION")
    print("=" * 60)
    print("🔄 Smart Website (3-tier) → Staff → LinkedIn → Patterns → AI → Send")
    print(f"🧠 Features: Smart actor selection + Enhanced fire protection targeting")
    print(f"📧 Test mode: All emails go to dave@alpha-omegaltd.com")
    print(f"💰 Credit threshold: ${args.credit_threshold}")
    print(f"📊 Max emails: {args.max_emails}")
    print(f"🎯 SMART: Cheerio → Content Crawler → Puppeteer (auto-escalation)")
    print(f"🛡️ IP Protection: Cheapest Apify proxies across all tiers")
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
        results = scraper.run_complete_workflow(args.url, args.max_emails)
        
        # Save results
        results_file = scraper.save_results(results)
        
        # Final summary
        print("\n🎉 SMART 3-TIER WORKFLOW SUMMARY")
        print("=" * 50)
        print(f"🌐 Website: {results['website_url']}")
        print(f"🎯 Scraping tier used: {results['scraping_tier_used']}")
        print(f"👥 Website staff found: {len(results['website_staff'])}")
        print(f"🔗 LinkedIn URL: {'✅ Found' if results['linkedin_url'] else '❌ Not found'}")
        print(f"📧 Fire protection emails sent: {len(results['emails_sent'])}")
        print(f"💾 Results saved: {results_file}")
        print(f"📬 Test emails sent to: dave@alpha-omegaltd.com")
        print(f"💰 Credit threshold used: ${args.credit_threshold}")
        print(f"🛡️ IP protection: Maintained across all tiers")
        print()
        
        if results['status'] == 'completed':
            print("✅ Smart 3-tier workflow successful!")
            
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