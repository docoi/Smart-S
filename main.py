#!/usr/bin/env python3
"""
🚀 COMPLETE WORKFLOW SUPER SCRAPER - MODULAR VERSION
==================================================
✅ FIXED: Smart Fallback workflow for when LinkedIn fails
✅ FIXED: Priority-based golden pattern testing (test Directors first)
✅ FIXED: Dynamic URL handling and normalization
✅ FIXED: Email deduplication and limiting
✅ FIXED: MillionVerifier integration with different thresholds

Usage: python main.py --url https://www.d3events.co.uk/ --credit-threshold 4.85

Features:
✅ Website scraping with enhanced GPT-4o analysis + LinkedIn URL discovery
✅ LinkedIn employee discovery with smart email patterns
✅ Smart Fallback when LinkedIn fails (uses website staff + golden patterns)
✅ Priority-based pattern testing (Directors/Owners first)
✅ Real-time credit monitoring with automatic account switching
✅ Smart MillionVerifier with different thresholds for different sources
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
    """🚀 Complete workflow orchestrator with Smart Fallback"""
    
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

    def normalize_url(self, url: str) -> str:
        """🌐 Normalize URL format"""
        if not url:
            return url
            
        # Add protocol if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        # Parse URL
        parsed = urlparse(url)
        netloc = parsed.netloc
        
        # Fix double protocol issue
        if netloc.startswith('https://') or netloc.startswith('http://'):
            # Extract the actual domain from malformed URL
            parts = netloc.split('://')
            if len(parts) > 1:
                netloc = parts[-1]
        
        # Add www if missing (unless it's an IP or localhost)
        if netloc and not netloc.startswith('www.') and not netloc.replace('.', '').isdigit() and 'localhost' not in netloc:
            netloc = 'www.' + netloc
        
        # Reconstruct URL
        from urllib.parse import urlunparse
        normalized = urlunparse((parsed.scheme or 'https', netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
        
        return normalized

    def run_complete_workflow(self, website_url: str) -> dict:
        """🎯 Complete workflow: Website → LinkedIn → Smart Fallback → Emails → Send"""
        
        # Normalize URL first
        original_url = website_url
        normalized_url = self.normalize_url(website_url)
        
        print("\n🎯 STARTING COMPLETE WORKFLOW")
        print("=" * 60)
        print(f"🌐 Original URL: {original_url}")
        print(f"🌐 Normalized URL: {normalized_url}")
        print(f"🔄 Process: Website → Staff → LinkedIn → Patterns → AI → Send")
        print(f"📧 Test mode: All emails go to {self.test_email}")
        print(f"📊 Max emails: {self.max_emails}")
        print()
        
        results = {
            'website_url': normalized_url,
            'original_url': original_url,
            'website_staff': [],
            'linkedin_url': '',
            'linkedin_employees': [],
            'verified_contacts': [],
            'emails_sent': [],
            'status': 'started'
        }
        
        try:
            # PART 1: Website scraping with Smart Fallback data collection
            print("📋 PART 1: WEBSITE SCRAPING")
            print("-" * 40)
            website_data = self.website_scraper.scrape_website_for_staff_and_linkedin(normalized_url)
            
            if website_data['status'] == 'failed':
                print("❌ Website scraping failed completely")
                results['status'] = 'failed_website_scraping'
                return results
            
            results['website_staff'] = website_data['website_staff']
            results['linkedin_url'] = website_data['linkedin_url']
            domain = website_data['domain']
            
            print(f"✅ Part 1 complete: {len(website_data['website_staff'])} staff + LinkedIn URL: {'Found' if website_data['linkedin_url'] else 'Not found'}")
            
            # Cooling-off period before Part 2
            print("\n⏳ COOLING-OFF PERIOD: Waiting 3 seconds before LinkedIn phase...")
            time.sleep(3)
            
            # PART 2: LinkedIn pipeline OR Smart Fallback
            if website_data['linkedin_url']:
                print(f"\n🔗 PART 2: LINKEDIN PIPELINE")
                print("-" * 40)
                print(f"🏢 Using modular LinkedIn pipeline with advanced features")
                print(f"✅ Features: Actor 2 + 33 Golden Patterns + Smart MillionVerifier + Pattern Learning")
                
                verified_contacts = self.linkedin_scraper.scrape_linkedin_and_discover_emails(
                    website_data['linkedin_url'], domain
                )
                
                if verified_contacts:
                    # Apply email limits
                    verified_contacts = self.remove_duplicate_emails(verified_contacts)
                    results['linkedin_employees'] = verified_contacts
                    results['verified_contacts'] = verified_contacts
                    
                    # PART 3: AI email generation and sending
                    print(f"\n🤖 PART 3: AI EMAIL GENERATION & SENDING")
                    print("-" * 40)
                    emails_sent = self._generate_and_send_emails(verified_contacts, domain)
                    results['emails_sent'] = emails_sent
                    results['status'] = 'completed'
                    
                    return results
                else:
                    print("❌ LinkedIn pipeline found no verified contacts")
                    print("🔄 Falling back to Smart Fallback workflow...")
            else:
                print("❌ No LinkedIn URL found")
                print("🔄 Proceeding with Smart Fallback workflow...")
            
            # PART 2B: Smart Fallback Workflow
            print(f"\n🔄 PART 2B: SMART FALLBACK WORKFLOW")
            print("-" * 40)
            print(f"📋 Reason: {'LinkedIn pipeline failed or returned no results' if website_data['linkedin_url'] else 'No LinkedIn URL found'}")
            print(f"🎯 Using captured website data for fire protection targeting")
            
            verified_contacts = self._process_fallback_data(
                website_data['fallback_contacts'], 
                website_data['fallback_emails'], 
                domain
            )
            
            if not verified_contacts:
                print("❌ No contacts available from fallback data")
                results['status'] = 'failed_no_contacts'
                return results
            
            results['verified_contacts'] = verified_contacts
            
            # PART 3: AI email generation and sending
            print(f"\n🤖 PART 3: AI EMAIL GENERATION & SENDING")
            print("-" * 40)
            emails_sent = self._generate_and_send_emails(verified_contacts, domain)
            results['emails_sent'] = emails_sent
            results['status'] = 'completed'
            
            return results
            
        except Exception as e:
            print(f"❌ Workflow failed: {e}")
            import traceback
            traceback.print_exc()
            results['status'] = 'failed'
            results['error'] = str(e)
            return results

    def _process_fallback_data(self, fallback_contacts: list, fallback_emails: list, domain: str) -> list:
        """🔄 Process Smart Fallback data with priority-based golden pattern testing"""
        
        print("🔄 PROCESSING SMART FALLBACK DATA")
        print("=" * 50)
        print(f"📊 Processing {len(fallback_contacts)} captured contacts...")
        
        verified_contacts = []
        
        if not fallback_contacts:
            print("❌ No fallback contacts available")
            return []
        
        # Separate contacts with and without emails
        contacts_with_emails = [c for c in fallback_contacts if c.get('email')]
        contacts_without_emails = [c for c in fallback_contacts if not c.get('email')]
        
        print(f"📧 Contacts with emails: {len(contacts_with_emails)}")
        print(f"❌ Contacts without emails: {len(contacts_without_emails)}")
        
        # Process contacts that already have emails
        for contact in contacts_with_emails:
            print(f"   👤 Processing: {contact['name']} - {contact['title']}")
            print(f"   📧 Email found: {contact['email']}")
            
            # Verify email with MillionVerifier (accept risky for website emails)
            is_valid = self.millionverifier.smart_verify_email_fallback(contact['email'], domain)
            
            if is_valid:
                contact['email_verified'] = True
                contact['verification_status'] = 'verified'
                contact['fire_protection_score'] = self._calculate_fire_protection_score(contact['title'])
                contact['fire_protection_reason'] = self._get_fire_protection_reason(contact['title'])
                
                verified_contacts.append(contact)
                print(f"   ✅ Contact verified: {contact['name']} (Score: {contact['fire_protection_score']})")
            else:
                print(f"   ❌ Email verification failed: {contact['email']}")
        
        # Process contacts without emails using priority-based golden pattern testing
        if contacts_without_emails:
            print(f"\n🧠 PRIORITY-BASED GOLDEN PATTERN TESTING")
            print(f"🎯 Testing patterns on {len(contacts_without_emails)} contacts without emails")
            
            # Sort by priority score (highest first)
            contacts_without_emails.sort(key=lambda x: x.get('priority', 0), reverse=True)
            
            discovered_pattern = None
            pattern_index = None
            
            # Test golden patterns on highest priority contacts first
            for i, contact in enumerate(contacts_without_emails[:3], 1):  # Test top 3 contacts
                name = contact['name']
                title = contact['title']
                priority = contact.get('priority', 0)
                
                print(f"\n🧪 Testing patterns for HIGH PRIORITY contact {i}: {name}")
                print(f"   📋 Title: {title} (Priority: {priority})")
                
                name_parts = name.split()
                if len(name_parts) < 2:
                    print(f"   ⚠️ Cannot generate patterns for single name: {name}")
                    continue
                
                first_name = name_parts[0]
                last_name = name_parts[-1]
                
                # Generate all 33 golden patterns
                golden_emails = self._generate_all_golden_patterns(first_name, last_name, domain)
                
                print(f"   📧 Testing {len(golden_emails)} golden email patterns:")
                
                pattern_found = False
                for j, email in enumerate(golden_emails, 1):
                    print(f"      🔍 Testing pattern {j}/{len(golden_emails)}: {email}")
                    
                    if self.millionverifier.smart_verify_email(email, domain):
                        print(f"      ✅ GOLDEN PATTERN FOUND: {email}")
                        print(f"      🎯 SUCCESS: Pattern {j} worked on {name}!")
                        
                        # Update contact with found email
                        contact['email'] = email
                        contact['email_source'] = f'golden_pattern_{j}'
                        contact['email_verified'] = True
                        contact['verification_status'] = 'verified'
                        contact['fire_protection_score'] = self._calculate_fire_protection_score(title)
                        contact['fire_protection_reason'] = self._get_fire_protection_reason(title)
                        
                        verified_contacts.append(contact)
                        
                        # Learn this pattern for other contacts
                        discovered_pattern = self._extract_pattern_from_email(email, first_name, last_name)
                        pattern_index = j
                        
                        print(f"      🧠 LEARNED PATTERN: {discovered_pattern} (will apply to remaining contacts)")
                        pattern_found = True
                        break
                    else:
                        print(f"      ❌ Pattern invalid: {email}")
                
                if pattern_found:
                    break
                else:
                    print(f"   😞 No valid patterns found for {name}")
            
            # Apply discovered pattern to remaining contacts
            if discovered_pattern:
                print(f"\n🧠 APPLYING LEARNED PATTERN TO REMAINING CONTACTS")
                print(f"🎯 Pattern: {discovered_pattern}")
                
                remaining_contacts = [c for c in contacts_without_emails if not c.get('email')]
                
                for contact in remaining_contacts:
                    name = contact['name']
                    name_parts = name.split()
                    
                    if len(name_parts) < 2:
                        continue
                    
                    first_name = name_parts[0]
                    last_name = name_parts[-1]
                    
                    # Apply learned pattern
                    test_email = self._apply_pattern_to_name(discovered_pattern, first_name, last_name, domain)
                    
                    if test_email:
                        print(f"   🧪 Testing learned pattern for {name}: {test_email}")
                        
                        if self.millionverifier.smart_verify_email(test_email, domain):
                            contact['email'] = test_email
                            contact['email_source'] = 'pattern_learned'
                            contact['email_verified'] = True
                            contact['verification_status'] = 'verified'
                            contact['fire_protection_score'] = self._calculate_fire_protection_score(contact['title'])
                            contact['fire_protection_reason'] = self._get_fire_protection_reason(contact['title'])
                            
                            verified_contacts.append(contact)
                            print(f"   ✅ PATTERN SUCCESS: {name} - {test_email}")
                        else:
                            print(f"   ❌ Pattern failed for {name}: {test_email}")
        
        # Apply email limits and deduplication
        verified_contacts = self.remove_duplicate_emails(verified_contacts)
        
        print(f"\n🎯 SMART FALLBACK RESULTS:")
        print(f"   📊 Contacts processed: {len(fallback_contacts)}")
        print(f"   ✅ Verified contacts: {len(verified_contacts)}")
        print(f"   📧 Final selection: {min(len(verified_contacts), self.max_emails)} (limited by max_emails)")
        
        return verified_contacts[:self.max_emails]

    def remove_duplicate_emails(self, contacts: list) -> list:
        """📧 Remove duplicate emails and limit to max_emails"""
        
        if not contacts:
            return []
        
        print(f"\n📧 Email filtering results:")
        print(f"   📊 Total found: {len(contacts)}")
        
        # Remove duplicates by email address
        seen_emails = set()
        unique_contacts = []
        
        # Sort by fire protection score first (highest first)
        contacts.sort(key=lambda x: x.get('fire_protection_score', 0), reverse=True)
        
        for contact in contacts:
            email = contact.get('email', '')
            if email and email not in seen_emails:
                seen_emails.add(email)
                unique_contacts.append(contact)
        
        print(f"   🔄 After deduplication: {len(unique_contacts)}")
        
        # Limit to max emails
        limited_contacts = unique_contacts[:self.max_emails]
        print(f"   ✂️ After limiting to {self.max_emails}: {len(limited_contacts)}")
        
        return limited_contacts

    def _calculate_fire_protection_score(self, title: str) -> int:
        """🔥 Calculate fire protection relevance score"""
        
        if not title:
            return 30
            
        title_lower = title.lower()
        
        # Highest priority - Fire safety roles
        if any(keyword in title_lower for keyword in ['fire', 'safety', 'hse', 'health and safety']):
            return 100
            
        # Very high priority - Facilities management
        if any(keyword in title_lower for keyword in ['facilities', 'facility', 'building', 'maintenance', 'estate', 'property']):
            return 95
            
        # High priority - Operations
        if any(keyword in title_lower for keyword in ['operations', 'operational', 'ops', 'site manager', 'plant']):
            return 85
            
        # Medium-high priority - Senior management
        if any(keyword in title_lower for keyword in ['director', 'ceo', 'owner', 'founder', 'managing director']):
            return 75
            
        # Medium priority - General management
        if any(keyword in title_lower for keyword in ['manager', 'head', 'supervisor', 'lead']):
            return 65
            
        # Lower priority - Other roles
        return 40

    def _get_fire_protection_reason(self, title: str) -> str:
        """🔥 Get reason for fire protection targeting"""
        
        if not title:
            return "General contact"
            
        title_lower = title.lower()
        
        if any(keyword in title_lower for keyword in ['fire', 'safety', 'hse']):
            return "Fire safety professional - direct responsibility"
        elif any(keyword in title_lower for keyword in ['facilities', 'facility', 'building', 'maintenance']):
            return "Facilities management - building safety oversight"
        elif any(keyword in title_lower for keyword in ['operations', 'operational', 'ops']):
            return "Operations role - safety procedures responsibility"
        elif any(keyword in title_lower for keyword in ['director', 'ceo', 'owner', 'founder']):
            return "Senior leadership - fire safety compliance authority"
        elif any(keyword in title_lower for keyword in ['manager', 'head', 'supervisor']):
            return "Management role - safety standards responsibility"
        else:
            return "Business contact - fire protection decision maker"

    def _generate_all_golden_patterns(self, first_name: str, last_name: str, domain: str) -> list:
        """📧 Generate all 33 golden email patterns"""
        
        # All 33 golden patterns
        patterns = [
            "{first}", "{last}", "{f}{last}", "{first}.{last}", "{first}_{last}", 
            "{first}-{last}", "{f}.{last}", "{last}.{first}", "{last}_{first}", 
            "{last}-{first}", "{last}{first}", "{f}_{last}", "{f}-{last}", 
            "{first}.{l}", "{first}_{l}", "{first}-{l}", "{first}{l}", "{f}{l}", 
            "{first}{last}", "{last}{f}", "{f}.{l}", "{l}.{first}", "{l}{first}",
            "{first}{middle}{last}", "{first}.{middle}.{last}", "{f}{middle}{last}",
            "{first}_{middle}_{last}", "{first}-{middle}-{last}", "{middle}.{last}",
            "{first}.{middle}", "{f}.{middle}.{last}", "{first}{m}{last}", "{f}{m}{l}"
        ]
        
        # Clean and prepare names
        first = first_name.lower().strip()
        last = last_name.lower().strip()
        f = first[0] if first else ''
        l = last[0] if last else ''
        
        emails = []
        
        for pattern in patterns:
            try:
                # Replace placeholders
                email_pattern = pattern
                email_pattern = email_pattern.replace('{first}', first)
                email_pattern = email_pattern.replace('{last}', last)
                email_pattern = email_pattern.replace('{f}', f)
                email_pattern = email_pattern.replace('{l}', l)
                email_pattern = email_pattern.replace('{middle}', '')  # No middle name
                email_pattern = email_pattern.replace('{m}', '')  # No middle initial
                
                # Skip if pattern couldn't be filled
                if '{' in email_pattern or '}' in email_pattern or not email_pattern.strip():
                    continue
                
                # Create full email
                full_email = f"{email_pattern}@{domain}"
                emails.append(full_email)
                
            except Exception:
                continue
        
        # Remove duplicates while preserving order
        seen = set()
        unique_emails = []
        for email in emails:
            if email not in seen:
                seen.add(email)
                unique_emails.append(email)
        
        return unique_emails

    def _extract_pattern_from_email(self, email: str, first_name: str, last_name: str) -> str:
        """🧠 Extract pattern from successful email"""
        
        try:
            local_part = email.split('@')[0].lower()
            first = first_name.lower().strip()
            last = last_name.lower().strip()
            f = first[0] if first else ''
            l = last[0] if last else ''
            
            # Pattern mappings
            patterns = {
                first: "{first}",
                last: "{last}",
                f + last: "{f}{last}",
                first + "." + last: "{first}.{last}",
                first + "_" + last: "{first}_{last}",
                first + "-" + last: "{first}-{last}",
                f + "." + last: "{f}.{last}",
                last + "." + first: "{last}.{first}",
                last + "_" + first: "{last}_{first}",
                last + "-" + first: "{last}-{first}",
                last + first: "{last}{first}",
                f + "_" + last: "{f}_{last}",
                f + "-" + last: "{f}-{last}",
                first + "." + l: "{first}.{l}",
                first + "_" + l: "{first}_{l}",
                first + "-" + l: "{first}-{l}",
                first + l: "{first}{l}",
                f + l: "{f}{l}",
                first + last: "{first}{last}",
                last + f: "{last}{f}"
            }
            
            for local_pattern, template in patterns.items():
                if local_part == local_pattern:
                    return template
            
            return None
            
        except Exception:
            return None

    def _apply_pattern_to_name(self, pattern: str, first_name: str, last_name: str, domain: str) -> str:
        """🔧 Apply learned pattern to new name"""
        
        try:
            first = first_name.lower().strip()
            last = last_name.lower().strip()
            f = first[0] if first else ''
            l = last[0] if last else ''
            
            # Replace pattern placeholders
            email_local = pattern
            email_local = email_local.replace('{first}', first)
            email_local = email_local.replace('{last}', last)
            email_local = email_local.replace('{f}', f)
            email_local = email_local.replace('{l}', l)
            
            # Check if pattern was successfully applied
            if '{' in email_local or '}' in email_local:
                return None
            
            return f"{email_local}@{domain}"
            
        except Exception:
            return None

    def _generate_and_send_emails(self, verified_contacts: list, domain: str) -> list:
        """🤖 AI email generation and sending"""
        
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
        domain = urlparse(results['website_url']).netloc.replace('www.', '').replace('.', '_')
        
        # Determine pipeline type for filename
        pipeline_type = "linkedin_pipeline" if results.get('linkedin_url') and results.get('linkedin_employees') else "smart_fallback"
        
        # Main results file
        main_filename = f"output/complete_workflow_{domain}_{pipeline_type}_{timestamp}.csv"
        
        with open(main_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow(['Complete Workflow Results'])
            writer.writerow(['Timestamp', timestamp])
            writer.writerow(['Website', results['website_url']])
            writer.writerow(['LinkedIn', results['linkedin_url']])
            writer.writerow(['Pipeline', pipeline_type])
            writer.writerow(['Status', results['status']])
            writer.writerow([])
            
            # Website staff
            writer.writerow(['Website Staff Found'])
            writer.writerow(['Name', 'Title', 'Source'])
            for staff in results['website_staff']:
                writer.writerow([staff.get('name', ''), staff.get('title', ''), 'Website'])
            writer.writerow([])
            
            # All employees found (LinkedIn or Fallback)
            if results.get('linkedin_employees'):
                writer.writerow(['LinkedIn Employees Found'])
                writer.writerow(['Name', 'Title', 'Email', 'Fire Protection Score', 'Priority'])
                for employee in results['linkedin_employees']:
                    writer.writerow([
                        employee.get('name', ''),
                        employee.get('title', ''),
                        employee.get('email', ''),
                        employee.get('fire_protection_score', ''),
                        employee.get('priority', '')
                    ])
            else:
                writer.writerow(['Smart Fallback Contacts'])
                writer.writerow(['Name', 'Title', 'Email', 'Fire Protection Score', 'Source'])
                for contact in results.get('verified_contacts', []):
                    writer.writerow([
                        contact.get('name', ''),
                        contact.get('title', ''),
                        contact.get('email', ''),
                        contact.get('fire_protection_score', ''),
                        contact.get('source', '')
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
    
    parser = argparse.ArgumentParser(description="🚀 Complete Workflow Super Scraper - MODULAR VERSION")
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
        # Initialize scraper with max emails setting
        scraper = CompleteWorkflowSuperScraper(max_emails=args.max_emails)
        
        # Run complete workflow
        results = scraper.run_complete_workflow(args.url)
        
        # Save results
        results_file = scraper.save_results(results)
        
        # Final summary
        pipeline_type = "LinkedIn Pipeline" if results.get('linkedin_url') and results.get('linkedin_employees') else "Smart Fallback"
        
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
