"""
VOXMILL TEST HARNESS
Automated validation of all stress scenarios
"""

import sys
from pathlib import Path
from stress_scenarios import VoxmillDataFactory
from pdf_generator import VoxmillPDFGenerator

class VoxmillTestHarness:
    """Automated testing suite"""
    
    SCENARIOS = [
        'baseline_mayfair',
        'stress_long_labels',
        'stress_extreme_values',
        'stress_dense_tables',
        'stress_sparse',
        'stress_nulls',
        'stress_zero_properties',  # Edge case
        'stress_single_property'   # Edge case
    ]
    
    def __init__(self, output_dir='/tmp/voxmill_tests'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.factory = VoxmillDataFactory()
        self.results = []
    
    def run_all_scenarios(self):
        """Execute all test scenarios"""
        print("="*70)
        print("VOXMILL TEST HARNESS — EXECUTING ALL SCENARIOS")
        print("="*70)
        
        for scenario_name in self.SCENARIOS:
            print(f"\n[TEST] {scenario_name}")
            print("-" * 70)
            
            try:
                # Generate test data
                data = self.factory.generate_scenario(scenario_name)
                
                # Write test data to JSON
                import json
                data_path = self.output_dir / f"{scenario_name}_data.json"
                with open(data_path, 'w') as f:
                    json.dump(data, f, indent=2)
                
                # Generate PDF
                generator = VoxmillPDFGenerator(
                    template_dir='/opt/render/project/src',
                    output_dir=str(self.output_dir),
                    data_path=str(data_path)
                )
                
                pdf_path = generator.generate(
                    output_filename=f"{scenario_name}.pdf"
                )
                
                # Validate PDF
                validation_result = self.validate_pdf(pdf_path, data)
                
                self.results.append({
                    'scenario': scenario_name,
                    'status': 'PASS' if validation_result['success'] else 'FAIL',
                    'pdf_path': str(pdf_path),
                    'data_path': str(data_path),
                    'validation': validation_result
                })
                
                print(f"✅ PASS: {scenario_name}")
                
            except Exception as e:
                print(f"❌ FAIL: {scenario_name}")
                print(f"   Error: {str(e)}")
                
                self.results.append({
                    'scenario': scenario_name,
                    'status': 'FAIL',
                    'error': str(e)
                })
        
        # Print summary
        self.print_summary()
    
    def validate_pdf(self, pdf_path, data):
        """Validate generated PDF meets quality standards"""
        validation = {
            'success': True,
            'checks': {}
        }
        
        # Check 1: PDF file exists and has content
        if not pdf_path.exists():
            validation['success'] = False
            validation['checks']['file_exists'] = False
        else:
            file_size = pdf_path.stat().st_size
            validation['checks']['file_exists'] = True
            validation['checks']['file_size_kb'] = file_size / 1024
            
            if file_size < 100000:  # Less than 100KB is suspicious
                validation['success'] = False
                validation['checks']['size_warning'] = 'PDF too small'
        
        # Check 2: Data integrity
        property_count = len(data.get('properties', []))
        validation['checks']['property_count'] = property_count
        
        # Check 3: Required fields present
        required_fields = ['metadata', 'kpis', 'properties']
        for field in required_fields:
            validation['checks'][f'has_{field}'] = field in data
            if field not in data:
                validation['success'] = False
        
        return validation
    
    def print_summary(self):
        """Print test results summary"""
        print("\n" + "="*70)
        print("TEST SUMMARY")
        print("="*70)
        
        passed = sum(1 for r in self.results if r['status'] == 'PASS')
        failed = sum(1 for r in self.results if r['status'] == 'FAIL')
        
        print(f"\nTotal Tests: {len(self.results)}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        
        if failed > 0:
            print("\nFailed Tests:")
            for result in self.results:
                if result['status'] == 'FAIL':
                    print(f"  • {result['scenario']}: {result.get('error', 'Unknown error')}")
        
        print("\nOutput Directory:", self.output_dir)
        print("="*70)

def main():
    harness = VoxmillTestHarness()
    harness.run_all_scenarios()

if __name__ == '__main__':
    main()
