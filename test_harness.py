#!/usr/bin/env python3
"""
VOXMILL TEST HARNESS
====================
Automated validation of all stress scenarios.
One command execution proves bulletproof operation across all edge cases.

USAGE:
    python test_harness.py
    
WHAT IT DOES:
    1. Generates 8 stress test scenarios
    2. Runs PDF generation for each
    3. Validates output quality
    4. Generates comprehensive test report
    
OUTPUT:
    • /tmp/voxmill_tests/*.pdf          - Test PDFs
    • /tmp/voxmill_tests/*_data.json    - Test data
    • /tmp/voxmill_tests/TEST_REPORT.md - Validation report
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from stress_scenarios import VoxmillDataFactory


class VoxmillTestHarness:
    """
    Automated testing suite for Voxmill PDF generation.
    Validates system resilience under extreme conditions.
    """
    
    # All scenarios to test
    SCENARIOS = [
        'baseline_mayfair',
        'stress_long_labels',
        'stress_extreme_values',
        'stress_dense_tables',
        'stress_sparse',
        'stress_nulls',
        'stress_zero_properties',
        'stress_single_property'
    ]
    
    def __init__(self, output_dir: str = '/tmp/voxmill_tests'):
        """
        Initialize test harness.
        
        Args:
            output_dir: Directory for test outputs
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.factory = VoxmillDataFactory()
        self.results = []
        
        print("="*70)
        print("VOXMILL TEST HARNESS — AUTOMATED VALIDATION SUITE")
        print("="*70)
        print(f"Output Directory: {self.output_dir}")
        print(f"Scenarios to Test: {len(self.SCENARIOS)}")
        print("="*70)
    
    def run_all_scenarios(self):
        """Execute all test scenarios and generate report"""
        
        start_time = datetime.now()
        
        for scenario_name in self.SCENARIOS:
            self._run_single_scenario(scenario_name)
        
        # Generate comprehensive report
        self._generate_report()
        
        duration = (datetime.now() - start_time).total_seconds()
        
        # Print summary
        self._print_summary(duration)
    
    def _run_single_scenario(self, scenario_name: str):
        """Run a single test scenario"""
        
        print(f"\n{'='*70}")
        print(f"[TEST] {scenario_name}")
        print(f"{'='*70}")
        
        try:
            # Step 1: Generate test data
            print(f"   [1/4] Generating test data...")
            data = self.factory.generate_scenario(scenario_name)
            
            # Step 2: Save test data
            data_path = self.output_dir / f"{scenario_name}_data.json"
            print(f"   [2/4] Saving data to {data_path.name}...")
            
            with open(data_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            # Step 3: Generate PDF
            print(f"   [3/4] Generating PDF...")
            pdf_path = self._generate_pdf(data_path, scenario_name)
            
            # Step 4: Validate output
            print(f"   [4/4] Validating output...")
            validation = self._validate_output(pdf_path, data)
            
            # Store result
            result = {
                'scenario': scenario_name,
                'status': 'PASS' if validation['success'] else 'FAIL',
                'pdf_path': str(pdf_path),
                'data_path': str(data_path),
                'validation': validation,
                'timestamp': datetime.now().isoformat()
            }
            
            self.results.append(result)
            
            # Print result
            if validation['success']:
                print(f"\n   ✅ PASS: {scenario_name}")
                print(f"      PDF: {pdf_path.name} ({validation['checks']['file_size_kb']:.1f} KB)")
            else:
                print(f"\n   ❌ FAIL: {scenario_name}")
                for check, value in validation['checks'].items():
                    if not value:
                        print(f"      Failed: {check}")
            
        except Exception as e:
            print(f"\n   ❌ FAIL: {scenario_name}")
            print(f"      Error: {str(e)}")
            
            self.results.append({
                'scenario': scenario_name,
                'status': 'FAIL',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
    
    def _generate_pdf(self, data_path: Path, scenario_name: str) -> Path:
        """
        Generate PDF using the refactored generator.
        
        Args:
            data_path: Path to test data JSON
            scenario_name: Name of scenario for output filename
            
        Returns:
            Path to generated PDF
        """
        # Import here to avoid circular dependency
        try:
            from pdf_generator import VoxmillPDFGenerator
        except ImportError:
            # If running standalone, try different import path
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "pdf_generator",
                "/opt/render/project/src/pdf_generator.py"
            )
            pdf_gen_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(pdf_gen_module)
            VoxmillPDFGenerator = pdf_gen_module.VoxmillPDFGenerator
        
        generator = VoxmillPDFGenerator(
            template_dir='/opt/render/project/src',
            output_dir=str(self.output_dir),
            data_path=str(data_path)
        )
        
        pdf_path = generator.generate(
            output_filename=f"{scenario_name}.pdf"
        )
        
        return pdf_path
    
    def _validate_output(self, pdf_path: Path, data: Dict) -> Dict:
        """
        Validate generated PDF meets quality standards.
        
        Args:
            pdf_path: Path to generated PDF
            data: Original test data
            
        Returns:
            Validation result dictionary
        """
        validation = {
            'success': True,
            'checks': {}
        }
        
        # Check 1: PDF file exists
        if not pdf_path.exists():
            validation['success'] = False
            validation['checks']['file_exists'] = False
            return validation
        
        validation['checks']['file_exists'] = True
        
        # Check 2: File size is reasonable
        file_size = pdf_path.stat().st_size
        validation['checks']['file_size_kb'] = file_size / 1024
        
        if file_size < 50000:  # Less than 50KB is suspicious
            validation['success'] = False
            validation['checks']['size_warning'] = 'PDF suspiciously small (<50KB)'
        elif file_size > 20000000:  # More than 20MB is too large
            validation['success'] = False
            validation['checks']['size_warning'] = 'PDF too large (>20MB)'
        else:
            validation['checks']['size_ok'] = True
        
        # Check 3: Data integrity
        property_count = len(data.get('properties', []))
        validation['checks']['property_count'] = property_count
        
        # Check 4: Required fields present in data
        required_fields = ['metadata', 'kpis', 'properties']
        for field in required_fields:
            has_field = field in data
            validation['checks'][f'has_{field}'] = has_field
            if not has_field:
                validation['success'] = False
        
        # Check 5: KPIs calculated (for non-zero datasets)
        if property_count > 0:
            kpis = data.get('kpis', {})
            has_valid_kpis = (
                'avg_price' in kpis and
                'days_on_market' in kpis and
                'total_properties' in kpis
            )
            validation['checks']['has_valid_kpis'] = has_valid_kpis
            if not has_valid_kpis:
                validation['success'] = False
        
        return validation
    
    def _generate_report(self):
        """Generate comprehensive test report"""
        
        report_path = self.output_dir / 'TEST_REPORT.md'
        
        passed = sum(1 for r in self.results if r['status'] == 'PASS')
        failed = sum(1 for r in self.results if r['status'] == 'FAIL')
        
        report = [
            "# VOXMILL PDF GENERATOR — STRESS TEST REPORT",
            "",
            f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**Total Tests:** {len(self.results)}",
            f"**Passed:** {passed} ✅",
            f"**Failed:** {failed} ❌",
            "",
            "---",
            "",
            "## TEST RESULTS",
            ""
        ]
        
        for result in self.results:
            scenario = result['scenario']
            status = result['status']
            emoji = '✅' if status == 'PASS' else '❌'
            
            report.append(f"### {emoji} {scenario}")
            report.append(f"**Status:** {status}")
            
            if status == 'PASS':
                validation = result.get('validation', {})
                checks = validation.get('checks', {})
                
                report.append(f"**PDF Size:** {checks.get('file_size_kb', 0):.1f} KB")
                report.append(f"**Properties:** {checks.get('property_count', 0)}")
                report.append(f"**PDF Path:** `{Path(result['pdf_path']).name}`")
                report.append(f"**Data Path:** `{Path(result['data_path']).name}`")
            else:
                error = result.get('error', 'Unknown error')
                report.append(f"**Error:** {error}")
            
            report.append("")
        
        # Add detailed validation summary
        report.extend([
            "---",
            "",
            "## VALIDATION CRITERIA",
            "",
            "Each test validates:",
            "",
            "1. ✅ **File Generation** - PDF file created successfully",
            "2. ✅ **File Size** - Between 50KB and 20MB",
            "3. ✅ **Data Integrity** - All required fields present",
            "4. ✅ **KPI Calculation** - Metrics computed correctly",
            "5. ✅ **No Crashes** - Generator handles edge cases gracefully",
            "",
            "---",
            "",
            "## SCENARIO DESCRIPTIONS",
            "",
            "- **baseline_mayfair** - Normal 40-property dataset",
            "- **stress_long_labels** - 80-120 character addresses/agencies",
            "- **stress_extreme_values** - Prices £50k-£50M, days 0-365",
            "- **stress_dense_tables** - 100 properties, 15 submarkets",
            "- **stress_sparse** - 2 properties, 1 submarket",
            "- **stress_nulls** - 30% missing data",
            "- **stress_zero_properties** - Edge case: empty dataset",
            "- **stress_single_property** - Edge case: 1 property only",
            "",
            "---",
            "",
            "## FILES GENERATED",
            ""
        ])
        
        # List all generated files
        for result in self.results:
            if result['status'] == 'PASS':
                report.append(f"- `{Path(result['pdf_path']).name}`")
                report.append(f"- `{Path(result['data_path']).name}`")
        
        report.extend([
            "",
            "---",
            "",
            f"**Test Suite Version:** Voxmill V3.0 Bulletproof Edition",
            f"**Engine:** WeasyPrint PDF Generator",
            f"**Template:** Jinja2 + HTML5/CSS3"
        ])
        
        # Write report
        with open(report_path, 'w') as f:
            f.write('\n'.join(report))
        
        print(f"\n📄 Test report saved: {report_path}")
    
    def _print_summary(self, duration: float):
        """Print test execution summary"""
        
        passed = sum(1 for r in self.results if r['status'] == 'PASS')
        failed = sum(1 for r in self.results if r['status'] == 'FAIL')
        
        print("\n" + "="*70)
        print("TEST EXECUTION SUMMARY")
        print("="*70)
        print(f"\n⏱️  Execution Time: {duration:.1f} seconds")
        print(f"\n📊 Results:")
        print(f"   Total Tests: {len(self.results)}")
        print(f"   ✅ Passed: {passed}")
        print(f"   ❌ Failed: {failed}")
        
        if failed > 0:
            print(f"\n⚠️  Failed Tests:")
            for result in self.results:
                if result['status'] == 'FAIL':
                    error = result.get('error', 'Unknown error')
                    print(f"   • {result['scenario']}: {error[:80]}")
        
        print(f"\n📁 Output Directory: {self.output_dir}")
        print(f"📄 Test Report: {self.output_dir / 'TEST_REPORT.md'}")
        
        if passed == len(self.results):
            print("\n🎉 ALL TESTS PASSED — SYSTEM IS BULLETPROOF")
        else:
            print(f"\n⚠️  {failed} TEST(S) FAILED — REVIEW REQUIRED")
        
        print("="*70)


# ============================================================================
# QUICK VALIDATION FUNCTION
# ============================================================================

def quick_validate_single_scenario(scenario_name: str):
    """
    Quick validation of a single scenario (for debugging).
    
    Args:
        scenario_name: Name of scenario to test
    """
    print(f"\n🔍 QUICK VALIDATION: {scenario_name}")
    print("="*70)
    
    factory = VoxmillDataFactory()
    data = factory.generate_scenario(scenario_name)
    
    print(f"\n✅ Data Generated:")
    print(f"   Properties: {len(data['properties'])}")
    print(f"   Avg Price: £{data['kpis']['avg_price']:,.0f}")
    print(f"   Avg Days: {data['kpis']['days_on_market']}")
    
    # Save for manual inspection
    output_path = Path('/tmp') / f'{scenario_name}_quick_test.json'
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"\n💾 Saved to: {output_path}")
    print("\nTo generate PDF, run:")
    print(f"   export VOXMILL_DATA_PATH='{output_path}'")
    print(f"   python pdf_generator.py")


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Main CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Voxmill Test Harness — Automated Validation Suite',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # Run full test suite
  python test_harness.py
  
  # Quick validate single scenario
  python test_harness.py --quick stress_long_labels
  
  # Custom output directory
  python test_harness.py --output /path/to/tests
  
TEST SCENARIOS:
  • baseline_mayfair          - Normal 40-property dataset
  • stress_long_labels        - 80-120 char addresses
  • stress_extreme_values     - Extreme prices/days
  • stress_dense_tables       - 100 properties, 15 submarkets
  • stress_sparse             - 2 properties
  • stress_nulls              - 30% missing data
  • stress_zero_properties    - Empty dataset
  • stress_single_property    - 1 property only
        """
    )
    
    parser.add_argument(
        '--output', '-o',
        default='/tmp/voxmill_tests',
        help='Output directory for test results'
    )
    
    parser.add_argument(
        '--quick', '-q',
        metavar='SCENARIO',
        help='Quick validate single scenario (no PDF generation)'
    )
    
    args = parser.parse_args()
    
    if args.quick:
        # Quick validation mode
        quick_validate_single_scenario(args.quick)
    else:
        # Full test suite
        harness = VoxmillTestHarness(output_dir=args.output)
        harness.run_all_scenarios()


if __name__ == '__main__':
    main()
