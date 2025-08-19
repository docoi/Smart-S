"""
🔗 LinkedIn Scraper Module - ENHANCED VERSION
============================================
✅ FIXED: Fire protection scoring only targets senior decision makers
✅ FIXED: Enhanced priority targeting (minimum score 80 required)
✅ FIXED: Better qualification criteria for purchasing authority
"""

import time
from urllib.parse import urlparse
from account_manager import ApifyAccountManager, MillionVerifierManager


class LinkedInScraper:
    """🔗 LinkedIn scraping with enhanced fire protection targeting"""
    
    def __init__(self, openai_key, millionverifier_manager):
        self.openai_key = openai_key
        self.millionverifier = millionverifier_manager
        
        # Pattern learning storage
        self.discovered_email_pattern = None
        self.discovered_pattern_index = None
    
    def scrape_linkedin_and_discover_emails(self, linkedin_url: str, domain: str) -> list:
        """🔍 Main method: Scrape LinkedIn and discover emails with enhanced targeting"""
        
        try:
            # Step 1: Native Actor 2 scraping with smart email discovery
            print("\n🔍 STEP 1: NATIVE ACTOR 2 SCRAPING WITH SMART EMAIL DISCOVERY")
            linkedin_contacts = self._native_scrape_linkedin_actor2(linkedin_url, domain)
            
            if not linkedin_contacts:
                print("❌ No employees found with Native Actor 2")
                return []
            
            # Cooling-off period before pattern application
            print("\n⏳ COOLING-OFF PERIOD: Waiting 2 seconds after pattern discovery...")
            time.sleep(2)
            
            # Step 1.5: Apply learned pattern to ALL employees
            if self.discovered_email_pattern:
                print(f"\n🚀 STEP 1.5: APPLYING LEARNED PATTERN TO ALL EMPLOYEES")
                linkedin_contacts = self._apply_pattern_to_all_employees(linkedin_contacts, domain)
            
            # Step 2: Enhanced fire protection targeting (SENIOR DECISION MAKERS ONLY)
            print("\n🎯 STEP 2: ENHANCED FIRE PROTECTION TARGETING (SENIOR DECISION MAKERS ONLY)")
            fire_targets = self._score_fire_protection_targets_enhanced(linkedin_contacts)
            
            # Step 3: Final email discovery for any remaining targets without emails
            print("\n📧 STEP 3: FINAL EMAIL DISCOVERY WITH GOLDEN PATTERNS")
            verified_contacts = self._discover_emails_golden_patterns(fire_targets, domain)
            
            return verified_contacts
            
        except Exception as e:
            print(f"❌ LinkedIn pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _native_scrape_linkedin_actor2(self, linkedin_url: str, domain: str) -> list:
        """🔍 Native Actor 2 scraping with smart priority-based pattern discovery"""
        
        print("🎯 NATIVE LINKEDIN ACTOR 2 SCRAPER")
        print("🔍 Target: Company employees with built-in email finding + Golden patterns fallback")
        print(f"🔗 LinkedIn URL: {linkedin_url}")
        print("=" * 70)
        
        try:
            # Get Apify client with account management
            manager = ApifyAccountManager()
            
            # 🔧 FIXED: Use the correct method for Part 2 (LinkedIn)
            try:
                client = manager.get_client_part2()
            except AttributeError:
                # Fallback if Part 2 method doesn't exist
                client = manager.get_client_part1()
            
            # Native Actor 2 configuration for email finding
            actor_input = {
                "companies": [linkedin_url],
                "maxItems": 30,
                "mode": "full_email",
                "includeEmails": True,
                "timeout": 120
            }
            
            print("🚀 Running Native Actor 2 with email configuration...")
            print(f"📧 Configuration: mode=full_email, includeEmails=True")
            print(f"💰 Estimated cost: ~${actor_input['maxItems'] * 12 / 1000:.2f}")
            
            # Run Actor 2
            run = client.actor("harvestapi/linkedin-company-employees").call(run_input=actor_input)
            
            # Record usage
            if hasattr(client, '_account_info'):
                manager.record_usage(client._account_info, success=(run is not None))
            
            if not run:
                print("❌ Native Actor 2 run failed")
                return []
            
            # Process results
            items = client.dataset(run["defaultDatasetId"]).list_items().items
            print(f"📊 Processing {len(items)} results from Native Actor 2...")
            
            processed_employees = []
            
            for item in items:
                try:
                    # Extract employee data
                    name = f"{item.get('firstName', '')} {item.get('lastName', '')}".strip()
                    title = item.get('headline', '') or item.get('position', '') or 'Employee'
                    email = (item.get('email', '') or 
                           item.get('emailAddress', '') or 
                           item.get('contactEmail', ''))
                    
                    # Person validation using GPT-4o-mini
                    if not self._is_real_person_gpt(name, title, domain):
                        print(f"   ⚠️ Skipping non-person account: {name}")
                        continue
                    
                    employee = {
                        'name': name,
                        'title': title,
                        'email': email.strip() if email else '',
                        'linkedin_profile_url': item.get('linkedinUrl', ''),
                        'location': item.get('location', {}).get('linkedinText', '') if isinstance(item.get('location'), dict) else str(item.get('location', '')),
                        'company': item.get('currentPosition', [{}])[0].get('companyName', '') if item.get('currentPosition') else '',
                        'priority': self._determine_priority(title),
                        'source': 'native_actor2'
                    }
                    
                    processed_employees.append(employee)
                    
                    if email:
                        print(f"   📧 EMAIL FOUND: {name} - {email}")
                        
                        # Learn pattern from Actor 2 email immediately
                        if not self.discovered_email_pattern:
                            name_parts = name.split()
                            if len(name_parts) >= 2:
                                first_name = name_parts[0]
                                last_name = name_parts[-1]
                                pattern = self._extract_pattern_from_email(email, first_name, last_name, domain)
                                if pattern:
                                    self.discovered_email_pattern = pattern
                                    self.discovered_pattern_index = "actor2"
                                    print(f"   🧠 LEARNED PATTERN from Actor 2: {pattern}")
                    else:
                        print(f"   📋 Processing: {name} | {title} | ❌")
                        print(f"   ⚠️ EMPLOYEE NO EMAIL: {name} - will try golden patterns")
                    
                except Exception as e:
                    print(f"   ❌ Error processing profile: {e}")
                    continue
            
            # Count emails from Actor 2
            with_emails_actor2 = [e for e in processed_employees if e.get('email')]
            print(f"\n🎯 NATIVE ACTOR 2 RESULTS:")
            print(f"📊 Total employee profiles found: {len(processed_employees)}")
            print(f"📧 Profiles with emails from Actor 2: {len(with_emails_actor2)}")
            print(f"📊 Actor 2 email success rate: {(len(with_emails_actor2)/len(processed_employees)*100):.1f}%" if processed_employees else "0.0%")
            
            # If Actor 2 found no emails, test golden patterns with smart prioritization
            if len(with_emails_actor2) == 0 and processed_employees:
                print("\n🔥 TESTING ALL 33 GOLDEN EMAIL PATTERNS:")
                
                employees_without_emails = [e for e in processed_employees if not e.get('email')]
                
                if employees_without_emails:
                    print("🧠 PRIORITIZING CONTACTS FOR PATTERN TESTING")
                    
                    # Add priority scores to employees for pattern testing
                    for employee in employees_without_emails:
                        title = employee.get('title', '').lower()
                        priority_score = self._calculate_pattern_test_priority(title)
                        employee['pattern_test_priority'] = priority_score
                        print(f"   📊 {employee.get('name', 'Unknown')} - {employee.get('title', 'Unknown')} | Priority: {priority_score}")
                    
                    # Sort by priority (highest first)
                    employees_without_emails.sort(key=lambda x: x.get('pattern_test_priority', 0), reverse=True)
                    
                    print(f"\n🎯 TOP 5 PRIORITY CONTACTS FOR PATTERN TESTING:")
                    for i, employee in enumerate(employees_without_emails[:5], 1):
                        name = employee.get('name', 'Unknown')
                        title = employee.get('title', 'Unknown Role')
                        score = employee.get('pattern_test_priority', 0)
                        print(f"   {i}. {name} - {title} (Score: {score})")
                    
                    # Test golden patterns on high-priority employees (up to 5)
                    pattern_found = False
                    max_tests = 5
                    
                    for test_num, test_employee in enumerate(employees_without_emails[:max_tests], 1):
                        name_parts = test_employee['name'].split()
                        
                        if len(name_parts) >= 2:
                            first_name = name_parts[0]
                            last_name = name_parts[-1]
                            middle_name = " ".join(name_parts[1:-1]) if len(name_parts) > 2 else ""
                            priority_score = test_employee.get('pattern_test_priority', 0)
                            
                            print(f"\n🧪 Testing ALL golden patterns for: {first_name} {middle_name} {last_name} @ {domain}".replace("  ", " "))
                            print(f"   📋 Title: {test_employee.get('title', 'Unknown')} (Priority Score: {priority_score})")
                            print(f"   🔢 Testing candidate {test_num}/{min(len(employees_without_emails), max_tests)}")
                            
                            # Generate all 33 golden patterns
                            golden_emails = self._generate_all_golden_patterns(first_name, last_name, domain, middle_name)
                            
                            print(f"📧 Testing {len(golden_emails)} golden email combinations:")
                            
                            for i, email in enumerate(golden_emails, 1):
                                print(f"   🔍 Testing golden pattern {i}/{len(golden_emails)}: {email}")
                                if self.millionverifier.smart_verify_email(email, domain):
                                    print(f"   ✅ GOLDEN PATTERN FOUND: {email}")
                                    print(f"   🎯 SUCCESS: Pattern {i} worked!")
                                    
                                    # Update the employee with found email
                                    test_employee['email'] = email
                                    test_employee['email_source'] = f'golden_pattern_{i}'
                                    
                                    # Learn this pattern for later use
                                    pattern = self._extract_pattern_from_email(email, first_name, last_name, domain)
                                    if pattern:
                                        self.discovered_email_pattern = pattern
                                        self.discovered_pattern_index = i
                                        print(f"   🧠 LEARNED PATTERN for later use: {pattern}")
                                    
                                    pattern_found = True
                                    break
                                else:
                                    print(f"   ❌ Golden pattern invalid: {email}")
                            
                            if pattern_found:
                                print(f"\n🎉 SUCCESS: Found email using golden pattern #{i} out of {len(golden_emails)} total patterns")
                                break
                            else:
                                print(f"   😞 No valid emails found for {test_employee['name']} after testing {len(golden_emails)} patterns")
                    
                    if not pattern_found:
                        print(f"\n😞 No valid emails found after testing {min(len(employees_without_emails), max_tests)} high-priority candidates")
            
            # Log discovered pattern for next phase
            if self.discovered_email_pattern:
                print(f"\n🧠 PATTERN DISCOVERED: {self.discovered_email_pattern} (will be applied to all employees)")
            else:
                print(f"\n⚠️ NO PATTERN DISCOVERED: Will test all patterns for each target")
            
            return processed_employees
            
        except Exception as e:
            print(f"❌ Native Actor 2 scraper failed: {e}")
            return []

    def _score_fire_protection_targets_enhanced(self, linkedin_contacts: list, max_targets: int = 2) -> list:
        """🎯 ENHANCED: Fire protection targeting - SENIOR DECISION MAKERS ONLY"""
        
        print("🔥 ENHANCED FIRE PROTECTION TARGETING - SENIOR DECISION MAKERS ONLY")
        print(f"🎯 Target: {max_targets} most senior fire protection decision-makers")
        print(f"⚖️ Minimum qualification score: 80 (senior management level)")
        print("=" * 70)
        
        # Only consider contacts that have verified emails
        contacts_with_emails = [c for c in linkedin_contacts if c.get('email')]
        
        print(f"📧 Analyzing {len(contacts_with_emails)} contacts with verified emails")
        
        if not contacts_with_emails:
            print("❌ No contacts with verified emails found")
            return []
        
        # 🔧 ENHANCED: Fire protection scoring criteria - SENIOR DECISION MAKERS ONLY
        scoring_criteria = {
            'owner_director': {
                'keywords': ['owner', 'founder', 'ceo', 'president', 'managing director', 'md', 'director'],
                'score': 100,
                'reason': 'Business owner/director - ultimate authority for fire safety investments and compliance'
            },
            'senior_management': {
                'keywords': ['manager', 'head of', 'general manager', 'operations manager', 'facilities manager', 'site manager'],
                'score': 90,
                'reason': 'Senior management - budget authority and responsibility for operational safety decisions'
            },
            'department_heads': {
                'keywords': ['head', 'lead', 'chief', 'supervisor', 'team leader'],
                'score': 80,
                'reason': 'Department head - influence over safety equipment purchasing and compliance decisions'
            }
        }
        
        qualified_contacts = []
        rejected_contacts = []
        
        for contact in contacts_with_emails:
            title = contact.get('title', '').lower()
            name = contact.get('name', '').lower()
            
            best_score = 0
            best_reason = 'Not qualified for fire protection decisions'
            
            # Score against each criteria
            for category, criteria in scoring_criteria.items():
                for keyword in criteria['keywords']:
                    if keyword in title or keyword in name:
                        if criteria['score'] > best_score:
                            best_score = criteria['score']
                            best_reason = criteria['reason']
                        break
            
            # Add scoring data
            contact['fire_protection_score'] = best_score
            contact['fire_protection_reason'] = best_reason
            
            # 🔧 ENHANCED: Only include contacts with scores above 80 (senior level)
            if best_score >= 80:
                qualified_contacts.append(contact)
                print(f"   ✅ QUALIFIED: {contact['name']} - {contact['title']} | Score: {best_score}")
                print(f"      📧 Email: {contact['email']} | 🎯 {best_reason}")
            else:
                rejected_contacts.append(contact)
                print(f"   ❌ NOT QUALIFIED: {contact['name']} - {contact['title']} | Score: {best_score}")
                print(f"      📧 Email: {contact['email']} | ❌ Not senior enough for purchasing decisions")
        
        print(f"\n📊 ENHANCED TARGETING RESULTS:")
        print(f"   ✅ Qualified senior decision makers: {len(qualified_contacts)}")
        print(f"   ❌ Rejected (not senior enough): {len(rejected_contacts)}")
        print(f"   📈 Qualification rate: {(len(qualified_contacts)/len(contacts_with_emails)*100):.1f}%" if contacts_with_emails else "0.0%")
        
        if not qualified_contacts:
            print("❌ No qualified senior decision makers found")
            print("💡 Consider expanding search to find directors, managers, or business owners")
            return []
        
        # Sort by score and select top targets
        qualified_contacts.sort(key=lambda x: x['fire_protection_score'], reverse=True)
        fire_targets = qualified_contacts[:max_targets]
        
        print(f"\n🎯 TOP {len(fire_targets)} QUALIFIED FIRE PROTECTION TARGETS:")
        for i, target in enumerate(fire_targets, 1):
            print(f"   {i}. {target['name']} - {target['title']}")
            print(f"      Score: {target['fire_protection_score']} | Email: {target['email']}")
            print(f"      🎯 {target['fire_protection_reason']}")
        
        return fire_targets

    def _is_real_person_gpt(self, name: str, title: str, company_name: str) -> bool:
        """🧠 Use GPT-4o-mini to determine if this is a real person or company account"""
        
        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_key)
            
            prompt = f"""Analyze if this is a REAL PERSON or a COMPANY ACCOUNT:

Name: "{name}"
Title: "{title}"
Company: "{company_name}"

Guidelines:
- REAL PERSON: Has first name + last name (e.g., "John Smith", "Maria Garcia", "李明", "Kathleen McDonagh")
- COMPANY ACCOUNT: Business names, departments, generic titles (e.g., "Go West", "Marketing Team", "Sales Dept", "Company Ltd")
- Consider cultural naming conventions globally

Answer with exactly one word: PERSON or COMPANY"""

            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=10,
                temperature=0.1
            )
            
            result = response.choices[0].message.content.strip().upper()
            is_person = "PERSON" in result
            
            print(f"   🧠 GPT-4o-mini: '{name}' = {'REAL PERSON' if is_person else 'COMPANY ACCOUNT'}")
            return is_person
            
        except Exception as e:
            print(f"   ⚠️ GPT validation failed: {e}, using fallback filter")
            return self._is_real_person_basic(name, title, company_name)
    
    def _is_real_person_basic(self, name: str, title: str, company_name: str) -> bool:
        """🔍 Basic code-based filtering as fallback"""
        
        name_lower = name.lower()
        company_lower = company_name.lower().replace('.com', '').replace('.co.uk', '').replace('.ie', '')
        
        # Skip obvious company accounts
        if (name_lower == company_lower or 
            name_lower.replace(' ', '') == company_lower.replace(' ', '') or
            name_lower in ['company', 'business', 'ltd', 'limited', 'inc', 'corp', 'team', 'department'] or
            len(name.split()) == 1 or  # Single word names
            any(word in name_lower for word in ['marketing', 'sales', 'support', 'team', 'dept', 'department'])):
            return False
        
        return True

    def _calculate_pattern_test_priority(self, title: str) -> int:
        """🎯 Calculate priority score for pattern testing (higher = test first)"""
        if not title:
            return 0
            
        title_lower = title.lower()
        
        # Highest priority - Senior leadership (most likely to have company emails)
        if any(keyword in title_lower for keyword in ['ceo', 'owner', 'founder', 'director', 'managing']):
            return 90
            
        # High priority - Management roles
        if any(keyword in title_lower for keyword in ['manager', 'head', 'lead', 'supervisor', 'account manager']):
            return 80
            
        # Medium-high priority - Core business roles
        if any(keyword in title_lower for keyword in ['specialist', 'coordinator', 'analyst', 'consultant']):
            return 60
            
        # Medium priority - Support roles
        if any(keyword in title_lower for keyword in ['assistant', 'support', 'associate', 'officer', 'representative']):
            return 40
            
        # Lower priority - Contract/freelance roles
        if any(keyword in title_lower for keyword in ['freelance', 'contractor', 'brand ambassador']):
            return 20
            
        # Lowest priority - Students/temporary roles
        if any(keyword in title_lower for keyword in ['student', 'intern', 'graduate', 'university']):
            return 10
            
        # Default for unclear roles
        return 30

    def _apply_pattern_to_all_employees(self, linkedin_contacts: list, domain: str) -> list:
        """🧠 Apply learned pattern to ALL employees with guaranteed email saving"""
        
        print("🧠 APPLYING LEARNED PATTERN TO ALL EMPLOYEES")
        
        # Get the discovered pattern
        pattern = getattr(self, 'discovered_email_pattern', None)
        if not pattern:
            print("⚠️ No pattern discovered - cannot apply to employees")
            return linkedin_contacts
            
        print(f"🎯 Pattern: {pattern}")
        print(f"📊 Testing pattern on {len(linkedin_contacts)} employees")
        print("=" * 70)
        
        # Track statistics
        emails_found = 0
        emails_failed = 0
        
        for contact in linkedin_contacts:
            # Skip if contact already has an email
            if contact.get('email'):
                continue
                
            name = contact.get('name', '')
            if not name:
                continue
            
            # Parse name for pattern application
            name_parts = name.split()
            if len(name_parts) < 2:
                continue
                
            first_name = name_parts[0]
            last_name = name_parts[-1]
            middle_name = " ".join(name_parts[1:-1]) if len(name_parts) > 2 else ""
            
            # Apply learned pattern
            test_email = self._apply_pattern_to_name(pattern, first_name, last_name, domain, middle_name)
            
            if not test_email:
                continue
                
            print(f"🧪 Testing learned pattern for {name}: {test_email}")
            
            # Verify email with MillionVerifier
            try:
                is_valid = self.millionverifier.smart_verify_email(test_email, domain)
                
                if is_valid:
                    # Update the original contact object directly
                    contact['email'] = test_email
                    contact['email_source'] = 'pattern_learned'
                    contact['email_verified'] = True
                    contact['pattern_used'] = pattern
                    contact['verification_status'] = 'verified'
                    
                    print(f"✅ PATTERN SUCCESS: {name} - {test_email}")
                    emails_found += 1
                else:
                    print(f"❌ Pattern failed for {name}: {test_email}")
                    emails_failed += 1
                    
            except Exception as e:
                print(f"❌ Error verifying {test_email}: {e}")
                emails_failed += 1
        
        # Verification step
        contacts_with_emails = [c for c in linkedin_contacts if c.get('email')]
        
        print(f"\n🎉 PATTERN APPLICATION RESULTS:")
        print(f"   🧠 Pattern applied: {pattern}")
        print(f"   ✅ New emails found: {emails_found}")
        print(f"   ❌ Pattern failures: {emails_failed}")
        print(f"   📊 Total contacts with emails: {len(contacts_with_emails)}")
        
        return linkedin_contacts

    def _discover_emails_golden_patterns(self, fire_targets: list, domain: str) -> list:
        """📧 Final email discovery using golden patterns fallback"""
        
        print("🧠 FINAL EMAIL DISCOVERY (GOLDEN PATTERNS FALLBACK)")
        print(f"🎯 Target: {len(fire_targets)} fire protection contacts")
        print("=" * 70)
        
        verified_contacts = []
        contacts_needing_emails = []
        
        # Separate contacts that already have emails vs those that need emails
        for contact in fire_targets:
            if contact.get('email') and contact.get('verification_status') == 'verified':
                print(f"✅ {contact['name']} already has verified email: {contact['email']}")
                verified_contacts.append(contact)
            else:
                contacts_needing_emails.append(contact)
                print(f"❌ {contact['name']} needs email discovery")
        
        if not contacts_needing_emails:
            print("\n🎉 All fire protection targets already have verified emails!")
            return verified_contacts
        
        print(f"\n🔧 GOLDEN PATTERN FALLBACK for {len(contacts_needing_emails)} contacts:")
        
        # Apply golden patterns to remaining contacts
        for contact in contacts_needing_emails:
            name_parts = contact['name'].split()
            if len(name_parts) < 2:
                print(f"   ⚠️ Cannot parse name '{contact['name']}' for pattern generation")
                continue
            
            first_name = name_parts[0]
            last_name = name_parts[-1]
            middle_name = " ".join(name_parts[1:-1]) if len(name_parts) > 2 else ""
            
            print(f"\n📧 DISCOVERING EMAIL FOR: {contact['name']}")
            
            # Generate and test all 33 golden patterns
            golden_emails = self._generate_all_golden_patterns(first_name, last_name, domain, middle_name)