"""
🔗 LinkedIn Scraper Module
=========================
Handles LinkedIn employee scraping, email pattern discovery, and email verification
"""

import time
from urllib.parse import urlparse
from account_manager import ApifyAccountManager, MillionVerifierManager


class LinkedInScraper:
    """🔗 LinkedIn scraping with smart pattern learning"""
    
    def __init__(self, openai_key, millionverifier_manager):
        self.openai_key = openai_key
        self.millionverifier = millionverifier_manager
        
        # Pattern learning storage
        self.discovered_email_pattern = None
        self.discovered_pattern_index = None
    
    def _determine_priority(self, title: str) -> str:
        """Determine employee priority based on title"""
        if not title:
            return 'standard'
            
        title_lower = title.lower()
        if any(keyword in title_lower for keyword in ['director', 'manager', 'head', 'chief', 'ceo', 'cto', 'cfo', 'vp', 'vice president', 'owner']):
            return 'high'
        elif any(keyword in title_lower for keyword in ['coordinator', 'specialist', 'lead', 'senior']):
            return 'medium'
        else:
            return 'standard'
    
    def scrape_linkedin_and_discover_emails(self, linkedin_url: str, domain: str) -> list:
        """🔍 Main method: Scrape LinkedIn and discover emails with pattern learning"""
        
        try:
            # Step 1: Native Actor 2 scraping with smart email discovery
            print("\n🔍 STEP 1: NATIVE ACTOR 2 SCRAPING WITH SMART EMAIL DISCOVERY")
            linkedin_contacts = self._native_scrape_linkedin_actor2(linkedin_url, domain)
            
            if not linkedin_contacts:
                print("❌ No employees found with Native Actor 2")
                return []
            
            # Cooling-off period after pattern discovery
            print("\n⏳ COOLING-OFF PERIOD: Waiting 2 seconds after pattern discovery...")
            time.sleep(2)
            
            # Step 1.5: Apply learned pattern to ALL employees
            if self.discovered_email_pattern:
                print(f"\n🚀 STEP 1.5: APPLYING LEARNED PATTERN TO ALL EMPLOYEES")
                linkedin_contacts = self._apply_pattern_to_all_employees(linkedin_contacts, domain)
            
            # Step 2: Fire protection targeting
            print("\n🎯 STEP 2: FIRE PROTECTION TARGETING")
            fire_targets = self._score_fire_protection_targets(linkedin_contacts)
            
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
            client = manager.get_client_part2()  # Changed from part1 to part2 since this is LinkedIn scraping
            
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
                    
                    # Add priority scores to employees
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

    def _score_fire_protection_targets(self, linkedin_contacts: list, max_targets: int = 2) -> list:
        """🎯 Fire protection targeting from verified emails"""
        
        print("🔥 FIRE PROTECTION CONTACT IDENTIFICATION")
        print(f"🎯 Target: {max_targets} most relevant fire protection decision-makers")
        print("=" * 70)
        
        # Only consider contacts that have verified emails
        contacts_with_emails = [c for c in linkedin_contacts if c.get('email')]
        
        print(f"📧 Analyzing {len(contacts_with_emails)} contacts with verified emails")
        
        if not contacts_with_emails:
            print("❌ No contacts with verified emails found")
            return []
        
        # Fire protection scoring criteria
        scoring_criteria = {
            'facilities': {
                'keywords': ['facilities', 'facility', 'building', 'maintenance', 'estate', 'property'],
                'score': 100,
                'reason': 'Facilities management - direct responsibility for building safety systems'
            },
            'safety': {
                'keywords': ['safety', 'health', 'hse', 'risk', 'compliance', 'security', 'fire'],
                'score': 100,
                'reason': 'Safety role - direct fire protection responsibility'
            },
            'operations': {
                'keywords': ['operations', 'operational', 'ops', 'site manager', 'plant'],
                'score': 85,
                'reason': 'Operations management - oversees safety procedures and equipment'
            },
            'management': {
                'keywords': ['manager', 'director', 'head', 'chief', 'md'],
                'score': 70,
                'reason': 'Management role - budget authority for safety investments'
            },
            'owner': {
                'keywords': ['owner', 'founder', 'ceo', 'president', 'managing director'],
                'score': 70,
                'reason': 'Business owner - ultimate responsibility for fire safety compliance'
            }
        }
        
        scored_contacts = []
        
        for contact in contacts_with_emails:
            title = contact.get('title', '').lower()
            name = contact.get('name', '').lower()
            
            best_score = 0
            best_reason = 'General contact'
            
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
            
            scored_contacts.append(contact)
            
            print(f"   📊 {contact['name']} - {contact['title']} | Score: {best_score} | {contact['email']} | {best_reason}")
        
        # Sort by score and select top targets
        scored_contacts.sort(key=lambda x: x['fire_protection_score'], reverse=True)
        fire_targets = scored_contacts[:max_targets]
        
        print(f"\n🎯 TOP {max_targets} FIRE PROTECTION TARGETS SELECTED:")
        for i, target in enumerate(fire_targets, 1):
            print(f"   {i}. {target['name']} - {target['title']}")
            print(f"      Score: {target['fire_protection_score']} | Email: {target['email']} | {target['fire_protection_reason']}")
        
        return fire_targets

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
            
            print(f"🧪 Testing {len(golden_emails)} golden email patterns:")
            
            email_found = False
            for i, email in enumerate(golden_emails, 1):
                print(f"   🔍 Testing pattern {i}/{len(golden_emails)}: {email}")
                
                if self.millionverifier.smart_verify_email(email, domain):
                    print(f"   ✅ GOLDEN PATTERN SUCCESS: {email}")
                    print(f"   🎯 MATCH: Pattern {i} worked for {contact['name']}!")
                    
                    contact['email'] = email
                    contact['email_source'] = f'golden_pattern_{i}'
                    contact['verification_status'] = 'verified'
                    verified_contacts.append(contact)
                    email_found = True
                    break
                else:
                    print(f"   ❌ Pattern invalid: {email}")
            
            if not email_found:
                print(f"   😞 No valid email found for {contact['name']} after testing {len(golden_emails)} patterns")
        
        print(f"\n📧 FINAL EMAIL DISCOVERY SUMMARY:")
        print(f"   🎯 Fire protection targets processed: {len(fire_targets)}")
        print(f"   ✅ Total verified email addresses: {len(verified_contacts)}")
        print(f"   📊 Overall success rate: {(len(verified_contacts)/len(fire_targets)*100):.1f}%" if fire_targets else "0.0%")
        
        return verified_contacts

    def _extract_pattern_from_email(self, email: str, first_name: str, last_name: str, domain: str) -> str:
        """🧠 Extract the pattern from a successful email"""
        try:
            local_part = email.split('@')[0].lower()
            first = first_name.lower().strip()
            last = last_name.lower().strip()
            f = first[0] if first else ''
            l = last[0] if last else ''
            
            # Common pattern mappings
            pattern_map = {
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
            
            # Find exact match
            for local_pattern, template in pattern_map.items():
                if local_part == local_pattern:
                    print(f"   🧠 Pattern extracted: {local_part} → {template}")
                    return template
            
            print(f"   ⚠️ Could not extract clear pattern from {email}")
            return None
            
        except Exception as e:
            print(f"   ⚠️ Could not extract pattern from {email}: {e}")
            return None

    def _apply_pattern_to_name(self, pattern: str, first_name: str, last_name: str, domain: str, middle_name: str = "") -> str:
        """🔧 Apply a learned pattern to a new name"""
        try:
            first = first_name.lower().strip()
            last = last_name.lower().strip()
            middle = middle_name.lower().strip() if middle_name else ""
            
            f = first[0] if first else ''
            l = last[0] if last else ''
            m = middle[0] if middle else ''
            
            # Replace pattern placeholders
            email_local = pattern
            email_local = email_local.replace('{first}', first)
            email_local = email_local.replace('{last}', last)
            email_local = email_local.replace('{middle}', middle)
            email_local = email_local.replace('{f}', f)
            email_local = email_local.replace('{l}', l)
            email_local = email_local.replace('{m}', m)
            
            # Check if pattern was successfully applied
            if '{' in email_local or '}' in email_local:
                print(f"   ⚠️ Pattern {pattern} could not be fully applied to {first} {last}")
                return None
            
            full_email = f"{email_local}@{domain}"
            return full_email
            
        except Exception as e:
            print(f"   ⚠️ Could not apply pattern {pattern}: {e}")
            return None

    def _generate_all_golden_patterns(self, first_name: str, last_name: str, domain: str, middle_name: str = "") -> list:
        """📧 Generate ALL 33 golden email patterns"""
        
        # Load all 33 golden patterns
        golden_patterns = [
            "{first}", "{last}", "{f}{last}", "{first}.{last}", "{first}_{last}", "{first}-{last}",
            "{f}.{last}", "{last}.{first}", "{last}_{first}", "{last}-{first}", "{last}{first}",
            "{f}_{last}", "{f}-{last}", "{first}.{l}", "{first}_{l}", "{first}-{l}", "{first}{l}",
            "{f}{l}", "{first}{m}{last}", "{first}{last}", "{last}{f}", "{first}@{domain}",
            "{last}@{domain}", "{first}.{last}@{domain}", "{f}{last}@{domain}", "{f}.{last}@{domain}",
            "{first}{last}@{domain}", "{first}-{last}@{domain}", "{first}_{last}@{domain}",
            "{first}{l}@{domain}", "{last}{f}@{domain}", "{f}{l}@{domain}", "{first}.{middle}.{last}@{domain}"
        ]
        
        # Clean and prepare names
        first = first_name.lower().strip()
        last = last_name.lower().strip()
        middle = middle_name.lower().strip() if middle_name else ""
        
        f = first[0] if first else ''
        l = last[0] if last else ''
        m = middle[0] if middle else ''
        
        emails = []
        
        for pattern in golden_patterns:
            try:
                # Replace placeholders
                email_pattern = pattern
                
                # Remove @{domain} suffix if present
                if '@{domain}' in email_pattern:
                    email_pattern = email_pattern.replace('@{domain}', '')
                
                # Replace all placeholders
                email_pattern = email_pattern.replace('{first}', first)
                email_pattern = email_pattern.replace('{last}', last)
                email_pattern = email_pattern.replace('{middle}', middle)
                email_pattern = email_pattern.replace('{f}', f)
                email_pattern = email_pattern.replace('{l}', l)
                email_pattern = email_pattern.replace('{m}', m)
                
                # Skip if pattern couldn't be filled
                if '{' in email_pattern or '}' in email_pattern or not email_pattern.strip():
                    continue
                
                # Create full email
                full_email = f"{email_pattern}@{domain}"
                emails.append(full_email)
                
            except Exception as e:
                continue
        
        # Remove duplicates
        seen = set()
        unique_emails = []
        for email in emails:
            if email not in seen:
                seen.add(email)
                unique_emails.append(email)
        
        return unique_emails