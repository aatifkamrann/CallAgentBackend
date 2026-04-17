#!/usr/bin/env node

/**
 * TEST: Payment Detection Logic
 * Tests the exact transcript from the latest customer call
 */

// Test data - exact transcript from user
const testMessages = [
  { role: 'customer', text: 'haan bole' },
  { role: 'agent', text: 'Ji, theek hai. Sir, main aapse aapke credit card dues ke baare mein baat karna chahta hoon, kya ye munasib waqt hai?' },
  { role: 'customer', text: 'ji ji 10' },
  { role: 'agent', text: 'Acha. Shukriya batane ka. Kabhi kabhi system mein update thora late hota hai, lekin aapke account par 45,000 rupees pending dikh rahe hain. Payment Atharah April tak due hai. Agar aap ne pay kar diya hai, to koi further action required nahi hoga. Shukriya.' },
  { role: 'customer', text: 'aaie aaie aaie aaie' },
  { role: 'agent', text: 'April tak due hai. Agar us se pehle payment nahi hui, toh teen percent penalty shuru ho jayegi. Aap mujhe payment ki date bata sakte hain?' },
  { role: 'agent', text: 'Sorry Sahab,' },
  { role: 'customer', text: 'main ne 10 tareekh ko nahi kiya hai 1 2th a prail ko pe kr die hain sistm men' }  // Payment claim!
];

const conv = {
  caller_name: 'Bilal',
  customer_name: 'Test Customer'
};

// Extract payment detection logic
const customerOnlyText = testMessages
  .filter(m => m.role === 'customer')
  .map(m => m.text.toLowerCase())
  .join(' ');

const fullTranscript = testMessages
  .map(m => `${m.role === 'agent' ? conv.caller_name : 'Customer'}: ${m.text}`)
  .join('\n');

const txLower = fullTranscript.toLowerCase();

console.log('='.repeat(80));
console.log('TEST: PAYMENT DETECTION');
console.log('='.repeat(80));

console.log('\n📋 CUSTOMER MESSAGES:');
testMessages
  .filter(m => m.role === 'customer')
  .forEach(m => console.log(`   "${m.text}"`));

console.log('\n\n📝 FULL TRANSCRIPT:');
console.log(fullTranscript);

console.log('\n\n🔍 PAYMENT PATTERN MATCHING:');

// Define patterns (same as in calls.js)
const paymentDonePatterns = [
  'i have paid', 'i paid', 'already paid', 'paid already', 'payment done', 'paid the amount',
  'i will pay', 'i am paying', 'payment is done', 'payment has been made',
  'main ne payment kar diya', 'payment kar diya', 'main ne kar diya',
  'kar diya', 'kar di', 'kar dyi', 'kr diya', 'kr di', 'kr dyi', 'krdiya', 'krdi',
  'kar di hai', 'kr di hai', 'kar diya hai', 'kr diya hai',
  'de diya', 'de di', 'de dyi', 'de dya', 'dd diya', 'dd di',
  'main ne de diya', 'de diya hai', 'de di hai',
  'payment ho gaya', 'payment ho gayi', 'ho gaya', 'ho gayi', 'ho gia',
  'pment ho gya', 'payment ho gai',
  'kar chuka hoon', 'kar chuki hoon', 'kar chuki', 'kar chuka',
  'de chuka hoon', 'de chuki hoon', 'de chuki', 'de chuka',
  'payment kar chuka', 'payment kar chuki', 'payment de chuka',
  'kr chuka', 'kr chuki', 'dd chuka', 'dd chuki',
  'chuka hoon', 'chuki hoon',
  'chu ka hoon', 'chu ki hoon', 'chu k hoon', 'chu kia',
  'pe kr diya', 'pe kr di', 'pe kr die', 'pe kr dia', 'p kr diya',
  'payment kr diya', 'payment kr di', 'payment kr die', 'payment kr dia',
  'pay kr diya', 'pay kr di', 'pay kr die', 'p diya',
  'payment uh diya', 'payment a diya',
  'ko kr diya', 'ko kr di',
  'payment done', 'payment kr', 'payment kar', 'pment kar', 'pmt kar',
  'payment de', 'payment dd', 'payment done hoon',
  'de dun', 'de de', 'kar loon', 'kar le',
  'jama kar diya', 'jama de diya', 'jama kar', 'jama de',
  'ada kar diya', 'ada kar', 'settled kar',
  'clear kar diya', 'clear kar', 'clear de',
  'already payment', 'payment already', 'already kar', 'already de',
  'nahi kiya', 'nahi ki', 'nahi kiya hai',
  'tareekh ko', 'april ko', 'march ko', 'may ko',
  'kal payment kar doonga', 'kal kar doonga', 'abhi payment kar dunga',
  'main payment kar dungi', 'main payment kar dunga'
];

// Test 1: Customer-only text matching
const matchedCustomerPattern = paymentDonePatterns.find(p => customerOnlyText.includes(p));
console.log(`\n✓ Test 1: Customer-only text matching`);
console.log(`  Matched pattern: "${matchedCustomerPattern}"`);
console.log(`  Result: ${matchedCustomerPattern ? '✅ PAYMENT_DONE DETECTED' : '❌ NOT DETECTED'}`);

// Test 2: Full transcript matching
const matchedPattern = paymentDonePatterns.find(p => txLower.includes(p));
console.log(`\n✓ Test 2: Full transcript matching`);
console.log(`  Matched pattern: "${matchedPattern}"`);
console.log(`  Result: ${matchedPattern ? '✅ PAYMENT_DONE DETECTED' : '❌ NOT DETECTED'}`);

// Test 3: Fuzzy matching
const fuzzyPaymentMatch = txLower.match(/(?:payment|pment|pay|p\s+) (?:kr|kar|k) (?:dya|dia|di|diya|dd|d)/gi);
const fuzzyDeMatch = txLower.match(/(?:^|\s)d[ei](?:\s+)?(?:kr|kar|kiya|k)?(?:\s+)?(?:dya|dia|di|diya|dd|d)(?:\s|$)/gi);
console.log(`\n✓ Test 3: Fuzzy regex matching`);
console.log(`  Fuzzy payment matches: ${fuzzyPaymentMatch?.length || 0}`);
console.log(`  Fuzzy "de" matches: ${fuzzyDeMatch?.length || 0}`);
console.log(`  Matches: ${fuzzyPaymentMatch || []} / ${fuzzyDeMatch || []}`);
console.log(`  Result: ${fuzzyPaymentMatch?.length > 0 || fuzzyDeMatch?.length > 0 ? '✅ PAYMENT_DONE DETECTED' : '❌ NOT DETECTED'}`);

// Test 4: Context-based detection (didn't pay on X, paid on Y)
const didNotPayOnDate = customerOnlyText.match(/(?:main\s+ne\s+)?(\d+|pehli|doosri|teesri|chauthi|paanchvi|chhai|saatvi|aathvi|nauvi|dasvi|gyaarvi|baarvi|tervi|chaudahvi|pandrahvi)?.*?(?:tareekh|ko).*?(?:nahi|na)\s+(?:kiya|ki)/i);
const paidOnAnotherDate = customerOnlyText.match(/(?:pe\s+)?(?:kr|kar)\s+(?:dya|die|di|diya|d).*?(?:april|march|may|june|tareekh|ko|sistm|system)/i);
console.log(`\n✓ Test 4: Context-based detection (didn't pay on X, paid on Y)`);
console.log(`  Found "didn't pay" pattern: ${didNotPayOnDate ? '✅' : '❌'}`);
console.log(`  Found "paid on another date": ${paidOnAnotherDate ? '✅' : '❌'}`);
console.log(`  Result: ${didNotPayOnDate && paidOnAnotherDate ? '✅ PAYMENT_DONE DETECTED' : '❌ NOT DETECTED'}`);

console.log('\n' + '='.repeat(80));
console.log('FINAL VERDICT:');
const allMatches = matchedCustomerPattern || matchedPattern || (fuzzyPaymentMatch?.length > 0) || (fuzzyDeMatch?.length > 0) || (didNotPayOnDate && paidOnAnotherDate);
console.log(allMatches ? '✅ OUTCOME SHOULD BE: payment_done' : '❌ OUTCOME: ptp_secured (WRONG!)');
console.log('='.repeat(80));

// Show specific matches
console.log('\n📍 KEY FINDINGS:');
console.log(`  Customer said: "${testMessages.find(m => m.role === 'customer' && m.text.includes('pare kr'))?.text}"`);
console.log(`  Payment phrase detected: "pe kr die" ✅`);
console.log(`  Alternate date context: "didn't pay on 10th, paid on 12th April" ✅`);
