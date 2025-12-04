#!/bin/bash
# Full pipeline integration test

echo "=========================================="
echo "VOXMILL FULL PIPELINE TEST"
echo "=========================================="

# Test 1: PDF Stress Test
echo -e "\n[TEST 1/3] Running PDF stress tests..."
python test_pdf_stress.py
if [ $? -ne 0 ]; then
    echo "❌ PDF stress test failed"
    exit 1
fi

# Test 2: PDF Generation (skip email)
echo -e "\n[TEST 2/3] Testing PDF generation..."
python voxmill_master.py --vertical uk-real-estate --area Mayfair --city London \
  --email test@test.com --name "Test" --skip-email
if [ $? -ne 0 ]; then
    echo "❌ PDF generation failed"
    exit 1
fi

# Test 3: Verify PDF output
echo -e "\n[TEST 3/3] Verifying PDF output..."
if [ ! -f "/tmp/Voxmill_Executive_Intelligence_Deck.pdf" ]; then
    echo "❌ PDF file not found"
    exit 1
fi

PDF_SIZE=$(stat -f%z "/tmp/Voxmill_Executive_Intelligence_Deck.pdf" 2>/dev/null || stat -c%s "/tmp/Voxmill_Executive_Intelligence_Deck.pdf")
if [ "$PDF_SIZE" -lt 50000 ]; then
    echo "❌ PDF too small ($PDF_SIZE bytes)"
    exit 1
fi

echo -e "\n=========================================="
echo "✅ ALL TESTS PASSED"
echo "=========================================="
echo "PDF Size: $PDF_SIZE bytes"
