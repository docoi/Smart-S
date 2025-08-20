#!/usr/bin/env python3
"""
🚀 COMPLETE WORKFLOW SUPER SCRAPER - UPDATED FOR APIFY_TOKEN_1
============================================================
✅ UPDATED: Uses APIFY_TOKEN_1 for all operations
✅ UPDATED: LinkedIn Actor 2 with Full + email search mode
✅ FIXED: Smart Fallback workflow with priority-based golden pattern testing
✅ FIXED: Real-time credit monitoring and account usage tracking
✅ FIXED: Enhanced staff extraction and email pattern learning

Usage: python main.py --url https://www.d3events.co.uk/ --credit-threshold 4.85

Features:
✅ Website scraping with enhanced GPT-4o analysis using APIFY_TOKEN_1
✅ LinkedIn Full + email search discovery ($12 per 1000) using APIFY_TOKEN_1
✅ Smart Fallback workflow when LinkedIn fails
✅ Priority-based golden pattern testing (test Directors/Owners first)
✅ Real-time MillionVerifier with smart verification
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
    from generate_patterns import generate_email_patterns
    print("✅ All modules imported successfully")
except ImportError as e:
    print(f"❌ Missing modules: {e}")
    print("🔧 Make sure all module files are in the same directory")
    sys.exit(1)


class CompleteWorkflowSuperScraper:
    """🚀 Complete workflow orchestrator - uses APIFY_TOKEN_1 for everything"""
    
    def __init__(self, max_emails=2):
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
        print(f"🛡️ Stealth: APIFY_TOKEN_1 with Full + email search subscription")
        print(f"📧 Test emails: {self.test_email}")
        print(f"📊 Max emails: {self.max_emails}")

    def run_complete_workflow(self, website_url: str) -> dict:
        """🎯 Complete workflow with Smart Fallback support"""
        
        # Normalize URL properly
        normalized_url = self._normalize_url(website_url)
        
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
            website_data = self.website_scraper.scrape_website_for_staff_and_linkedin(normalized_url)
            
            # Handle both old tuple format and new dict format
            if isinstance(website_data, tuple):
                website_staff, linkedin_url = website_data
                fallback_data = None
            else:
                website_staff = website_data.get('staff', [])
                linkedin_url = website_data.get('linkedin_url', '')
                fallback_data = website_data
            
            results['website_staff'] = website_staff
            results['linkedin_url'] = linkedin_url
            
            print(f"✅ Part 1 complete: {len(website_staff)} staff found")
            
            # Cooling-off period before Part 2
            print("\n⏳ COOLING-OFF PERIOD: Waiting 3 seconds before LinkedIn phase...")
            time.sleep(3)
            
            # PART 2: LinkedIn pipeline
            print(f"\n🔗 PART 2: LINKEDIN PIPELINE")
            print("-" * 40)
            
            if linkedin_url:
                print(f"🏢 LinkedIn URL: {linkedin_url}")
                print(f"💰 Using Full + email search mode with APIFY_TOKEN_1")
                
                # Extract domain for LinkedIn processing
                domain = urlparse(normalized_url).netloc.replace('www.', '')
                verified_contacts = self.linkedin_scraper.scrape_linkedin_and_discover_emails(linkedin_url, domain)
                
                results['linkedin_employees'] = verified_contacts
                results['verified_contacts'] = verified_contacts
                
                if verified_contacts:
                    print(f"✅ LinkedIn pipeline successful: {len(verified_contacts)} verified contacts")
                else:
                    print("⚠️ LinkedIn pipeline returned 0 contacts - triggering Smart Fallback")
                    verified_contacts = self._smart_fallback_workflow(website_staff, domain)
                    results['verified_contacts'] = verified_contacts
            else:
                print("❌ No LinkedIn URL found - triggering Smart Fallback")
                domain = urlparse(normalized_url).netloc.replace('www.', '')
                verified_contacts = self._smart_fallback_workflow(website_staff, domain)
                results['verified_contacts'] = verified_contacts
            
            if not verified_contacts:
                print("❌ No verified email addresses found")
                results['status'] = 'failed_no_emails'
                return results
            
            # Limit emails to max_emails setting
            verified_contacts = self._limit_and_deduplicate_contacts(verified_contacts)
            results['verified_contacts'] = verified_contacts
            
            # PART 3: AI email generation and sending
            print(f"\n🤖 PART 3: AI EMAIL GENERATION & SENDING")
            print("-" * 40)
            emails_sent = self._generate_and_send_emails(verified_contacts, domain)
            
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

    def _smart_fallback_workflow(self, website_staff: list, domain: str) -> list:
        """🚀 SMART FALLBACK: Priority-based golden pattern testing"""
        
        print("\n🚀 SMART FALLBACK WORKFLOW")
        print("=" * 60)
        print("🎯 Strategy: Test golden patterns on highest-priority staff first")
        print("🧠 Learn successful pattern, then apply to all other staff")
        print("💡 This reduces MillionVerifier usage and increases success rate")
        print()
        
        if not website_staff:
            print("❌ No website staff found for Smart Fallback")
            return []
        
        print(f"👥 Website staff available for Smart Fallback: {len(website_staff)}")
        for staff in website_staff:
            print(f"   📋 {staff.get('name', 'Unknown')} - {staff.get('title', 'Unknown')}")
        
        # Step 1: Priority scoring for staff
        print("\n🎯 STEP 1: PRIORITY SCORING")
        print("-" * 40)
        
        scored_staff = []
        for staff in website_staff:
            priority_score = self._calculate_fallback_priority(staff.get('title', ''), staff.get('name', ''))
            staff['fallback_priority'] = priority_score
            scored_staff.append(staff)
            print(f"📊 {staff.get('name', 'Unknown')} - Priority: {priority_score}")
        
        # Sort by priority (highest first)
        scored_staff.sort(key=lambda x: x.get('fallback_priority', 0), reverse=True)
        
        print(f"\n🏆 TOP PRIORITY STAFF FOR PATTERN TESTING:")
        for i, staff in enumerate(scored_staff[:3], 1):
            print(f"   {i}. {staff.get('name', 'Unknown')} - {staff.get('title', 'Unknown')} (Score: {staff.get('fallback_priority', 0)})")
        
        # Step 2: Priority-based pattern testing
        print("\n🧠 PRIORITY-BASED GOLDEN PATTERN TESTING")
        print("-" * 50)
        
        learned_pattern = None
        verified_contacts = []
        
        # Test patterns on highest priority staff first
        for i, staff in enumerate(scored_staff, 1):
            name = staff.get('name', '')
            if not name or len(name.split()) < 2:
                print(f"⚠️ Cannot test patterns for '{name}' - insufficient name data")
                continue
            
            print(f"\n🧪 Testing patterns for HIGH PRIORITY contact {i}: {name}")
            
            name_parts = name.split()
            first_name = name_parts[0]
            last_name = name_parts[-1]
            
            # Generate golden patterns
            golden_patterns = generate_email_patterns(first_name, last_name, domain)
            
            print(f"   📧 Testing {len(golden_patterns)} golden email patterns:")
            
            pattern_found = False
            for j, email in enumerate(golden_patterns, 1):
                print(f"      🔍 Testing pattern {j}/{len(golden_patterns)}: {email}")
                
                if self.millionverifier.smart_verify_email(email, domain):
                    print(f"      ✅ GOLDEN PATTERN FOUND: {email}")
                    print(f"      🧠 LEARNED PATTERN: {self._extract_pattern_from_golden(email, first_name, last_name, domain)} (will apply to remaining contacts)")
                    
                    # Learn the successful pattern
                    learned_pattern = self._extract_pattern_from_golden(email, first_name, last_name, domain)
                    
                    # Add to verified contacts with fire protection scoring
                    staff['email'] = email
                    staff['email_source'] = f'golden_pattern_{j}'
                    staff['fire_protection_score'] = self._score_fire_protection_relevance(staff.get('title', ''))
                    staff['fire_protection_reason'] = self._get_fire_protection_reason(staff.get('title', ''))
                    verified_contacts.append(staff)
                    
                    pattern_found = True
                    break
                else:
                    print(f"      ❌ Pattern invalid: {email}")
            
            if pattern_found:
                break
            else:
                print(f"   😞 No valid patterns found for {name}")
        
        # Step 3: Apply learned pattern to remaining staff
        if learned_pattern and len(scored_staff) > 1:
            print(f"\n🧠 APPLYING LEARNED PATTERN TO REMAINING CONTACTS")
            print(f"🎯 Pattern discovered: {learned_pattern}")
            print("-" * 50)
            
            remaining_staff = [s for s in scored_staff if s not in verified_contacts]
            
            for staff in remaining_staff:
                name = staff.get('name', '')
                if not name or len(name.split()) < 2:
                    continue
                
                name_parts = name.split()
                first_name = name_parts[0]
                last_name = name_parts[-1]
                
                # Apply learned pattern
                test_email = self._apply_learned_pattern(learned_pattern, first_name, last_name, domain)
                
                if test_email:
                    print(f"   🧪 Testing learned pattern for {name}: {test_email}")
                    
                    if self.millionverifier.smart_verify_email(test_email, domain):
                        print(f"   ✅ PATTERN SUCCESS: {name} - {test_email}")
                        
                        staff['email'] = test_email
                        staff['email_source'] = 'learned_pattern'
                        staff['fire_protection_score'] = self._score_fire_protection_relevance(staff.get('title', ''))
                        staff['fire_protection_reason'] = self._get_fire_protection_reason(staff.get('title', ''))
                        verified_contacts.append(staff)
                    else:
                        print(f"   ❌ Learned pattern failed for {name}: {test_email}")
        
        print(f"\n🎉 SMART FALLBACK RESULTS:")
        print(f"   🧠 Pattern learned: {'Yes' if learned_pattern else 'No'}")
        print(f"   ✅ Verified contacts: {len(verified_contacts)}")
        print(f"   📊 Success rate: {(len(verified_contacts)/len(website_staff)*100):.1f}%" if website_staff else "0.0%")
        
        return verified_contacts

    def _calculate_fallback_priority(self, title: str, name: str) -> int:
        """🎯 Calculate priority for Smart Fallback testing"""
        score = 0
        title_lower = title.lower() if title else ''
        name_lower = name.lower() if name else ''
        
        # Highest priority - Business owners and directors
        if any(keyword in title_lower for keyword in ['owner', 'founder', 'director', 'managing director', 'ceo']):
            score = 90
        # High priority - Management
        elif any(keyword in title_lower for keyword in ['manager', 'head', 'chief', 'lead']):
            score = 75
        # Medium priority - Specialists and coordinators
        elif any(keyword in title_lower for keyword in ['specialist', 'coordinator', 'analyst']):
            score = 50
        # Lower priority - Support roles
        elif any(keyword in title_lower for keyword in ['assistant', 'support', 'associate']):
            score = 25
        else:
            score = 10
        
        return score

    def _extract_pattern_from_golden(self, email: str, first_name: str, last_name: str, domain: str) -> str:
        """🧠 Extract pattern from successful golden pattern email"""
        try:
            local_part = email.split('@')[0]
            first = first_name.lower()
            last = last_name.lower()
            
            # Common pattern detection
            if local_part == f"{first}.{last}":
                return "{first}.{last}"
            elif local_part == f"{first}":
                return "{first}"
            elif local_part == f"{last}":
                return "{last}"
            elif local_part == f"{first}{last}":
                return "{first}{last}"
            elif local_part == f"{first[0]}{last}":
                return "{f}{last}"
            else:
                return f"{local_part}"  # Return as-is if no clear pattern
                
        except Exception as e:
            print(f"   ⚠️ Could not extract pattern from {email}: {e}")
            return None

    def _apply_learned_pattern(self, pattern: str, first_name: str, last_name: str, domain: str) -> str:
        """🔧 Apply learned pattern to new name"""
        try:
            first = first_name.lower()
            last = last_name.lower()
            f = first[0] if first else ''
            
            # Apply pattern
            if pattern == "{first}.{last}":
                local_part = f"{first}.{last}"
            elif pattern == "{first}":
                local_part = first
            elif pattern == "{last}":
                local_part = last
            elif pattern == "{first}{last}":
                local_part = f"{first}{last}"
            elif pattern == "{f}{last}":
                local_part = f"{f}{last}"
            else:
                # Pattern is a direct string, use as-is but substitute names
                local_part = pattern.replace('{first}', first).replace('{last}', last).replace('{f}', f)
            
            return f"{local_part}@{domain}"
            
        except Exception as e:
            print(f"   ⚠️ Could not apply pattern {pattern}: {e}")
            return None

    def _score_fire_protection_relevance(self, title: str) -> int:
        """🔥 Score contact for fire protection relevance"""
        if not title:
            return 25
        
        title_lower = title.lower()
        
        # Highest relevance - Direct responsibility
        if any(keyword in title_lower for keyword in ['owner', 'director', 'managing']):
            return 75
        # High relevance - Management with decision authority
        elif any(keyword in title_lower for keyword in ['manager', 'head', 'chief']):
            return 65
        # Medium relevance - Operational roles
        elif any(keyword in title_lower for keyword in ['coordinator', 'specialist']):
            return 45
        # Lower relevance - Support roles
        else:
            return 25

    def _get_fire_protection_reason(self, title: str) -> str:
        """🔥 Get reason for fire protection targeting"""
        if not title:
            return "General business contact"
        
        title_lower = title.lower()
        
        if any(keyword in title_lower for keyword in ['owner', 'founder']):
            return "Business owner - ultimate responsibility for fire safety compliance"
        elif any(keyword in title_lower for keyword in ['director', 'managing']):
            return "Senior management - budget authority for fire protection systems"
        elif any(keyword in title_lower for keyword in ['manager', 'head']):
            return "Management role - responsible for workplace safety procedures"
        else:
            return "Business contact - potential fire safety decision influence"

    def _limit_and_deduplicate_contacts(self, contacts: list) -> list:
        """📊 Limit to max emails and remove duplicates"""
        
        if not contacts:
            return []
        
        # Remove duplicates by email
        seen_emails = set()
        unique_contacts = []
        
        for contact in contacts:
            email = contact.get('email', '')
            if email and email not in seen_emails:
                seen_emails.add(email)
                unique_contacts.append(contact)
        
        # Sort by fire protection score
        unique_contacts.sort(key=lambda x: x.get('fire_protection_score', 0), reverse=True)
        
        # Limit to max_emails
        limited_contacts = unique_contacts[:self.max_emails]
        
        print(f"\n📊 EMAIL LIMITING & DEDUPLICATION:")
        print(f"   📧 Original contacts: {len(contacts)}")
        print(f"   🔄 After deduplication: {len(unique_contacts)}")
        print(f"   📊 After max limit ({self.max_emails}): {len(limited_contacts)}")
        
        return limited_contacts

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

    def _normalize_url(self, url: str) -> str:
        """🌐 Normalize URL format"""
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        # Remove trailing slash
        url = url.rstrip('/')
        
        return url

    def save_results(self, results: dict) -> str:
        """💾 Save comprehensive workflow results to output folder"""
        
        # Ensure output directory exists
        os.makedirs("output", exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain = urlparse(results['website_url']).netloc.replace('www.', '').replace('.', '_')
        
        # Main results file
        main_filename = f"output/complete_workflow_{domain}_linkedin_pipeline_{timestamp}.csv"
        
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
        
        print(f"💾 Main results saved: {main_filename}")
        
        return main_filename


def main():
    """🚀 CLI interface and main execution"""
    
    parser = argparse.ArgumentParser(description="🚀 Complete Workflow Super Scraper - UPDATED FOR APIFY_TOKEN_1")
    parser.add_argument("--url", required=True, help="Target website URL")
    parser.add_argument("--credit-threshold", type=float, default=4.85, 
                       help="Credit threshold for account switching (default: $4.85)")
    parser.add_argument("--max-emails", type=int, default=2,
                       help="Maximum emails to send (default: 2)")
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

    # Check environment variables
    required_vars = ['APIFY_TOKEN_1', 'OPENAI_API_KEY', 'MILLIONVERIFIER_API_KEY', 'SMTP_EMAIL', 'SMTP_PASSWORD']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("🔧 Please set these in your .env file")
        sys.exit(1)
    
    print("✅ All environment variables found")
    print()

    try:
        # Initialize scraper
        scraper = CompleteWorkflowSuperScraper(max_emails=args.max_emails)
        
        # Run complete workflow
        results = scraper.run_complete_workflow(args.url)
        
        # Save results
        results_file = scraper.save_results(results)
        
        # Final summary
        print("\n🎉 SMART FALLBACK WORKFLOW SUMMARY")
        print("=" * 50)
        print(f"🌐 Website: {results['website_url']}")
        print(f"👥 Website staff found: {len(results['website_staff'])}")
        print(f"🔗 LinkedIn URL: {'✅ Found' if results['linkedin_url'] else '❌ Not found'}")
        print(f"📧 Fire protection emails sent: {len(results['emails_sent'])}")
        print(f"📊 Max emails setting: {args.max_emails}")
        print(f"💾 Results saved: {results_file}")
        print(f"📬 Test emails sent to: dave@alpha-omegaltd.com")
        print(f"💰 Credit threshold used: ${args.credit_threshold}")
        print(f"⚠️ Workflow status: {results['status']}")
        
        if 'error' in results:
            print(f"❌ Error: {results['error']}")
        
        print()
        
        if results['status'] == 'completed':
            print("✅ Complete workflow successful!")
            
            if results['emails_sent']:
                print("\n🎯 Fire Protection Targets Contacted:")
                for email in results['emails_sent']:
                    print(f"   📧 {email['name']} - {email['title']} (Score: {email.get('fire_protection_score', 'N/A')})")
                    print(f"      Email: {email.get('email', 'N/A')} | Source: {email.get('email_source', 'N/A')}")
                    print()
        else:
            print(f"⚠️ Workflow status: {results['status']}")

    except KeyboardInterrupt:
        print("\n🛑 Workflow interrupted by user")
    except Exception as e:
        print(f"\n❌ Workflow failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()