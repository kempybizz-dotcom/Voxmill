#!/usr/bin/env python3
"""
VOXMILL MARKET INTELLIGENCE — PDF GENERATOR
Production-ready HTML/CSS to PDF converter using WeasyPrint
Fortune-500 grade report generation system
"""

import os
import sys
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from weasyprint import HTML, CSS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VoxmillPDFGenerator:
    """
    Fortune-500 grade PDF generator for Voxmill Market Intelligence reports.
    Renders HTML/CSS templates using Jinja2 and exports via WeasyPrint.
    """
    
    def __init__(
        self,
        template_dir: str = "/home/claude",
        output_dir: str = "/tmp",
        data_path: str = "/tmp/voxmill_analysis.json"
    ):
        """
        Initialize PDF generator with paths to templates and data.
        
        Args:
            template_dir: Directory containing HTML and CSS templates
            output_dir: Directory for PDF output
            data_path: Path to JSON data file
        """
        self.template_dir = Path(template_dir)
        self.output_dir = Path(output_dir)
        self.data_path = Path(data_path)
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=select_autoescape(['html', 'xml']),
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        logger.info(f"Initialized Voxmill PDF Generator")
        logger.info(f"Template directory: {self.template_dir}")
        logger.info(f"Output directory: {self.output_dir}")
    
    def load_data(self) -> Dict[str, Any]:
        """
        Load market intelligence data from JSON file.
        
        Returns:
            Dictionary containing all report data
        """
        logger.info(f"Loading data from {self.data_path}")
        
        if not self.data_path.exists():
            logger.error(f"Data file not found: {self.data_path}")
            raise FileNotFoundError(f"Data file not found: {self.data_path}")
        
        try:
            with open(self.data_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Successfully loaded {len(data)} data sections")
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in data file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def prepare_chart_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw data into chart-ready format for SVG rendering.
        
        Args:
            data: Raw market intelligence data
            
        Returns:
            Dictionary with formatted chart data
        """
        chart_data = {
            'price_distribution': [],
            'price_ranges': [],
            'weekly_trend': [],
            'market_share': [],
            'competitor_inventory': []
        }
        
        # Price distribution bars
        if 'price_distribution' in data:
            dist = data['price_distribution']
            chart_data['price_distribution'] = [
                {
                    'label': '£0-500k',
                    'count': dist.get('0_500k', 0),
                    'height': min(dist.get('0_500k', 0) * 3, 150)
                },
                {
                    'label': '£500k-1M',
                    'count': dist.get('500k_1m', 0),
                    'height': min(dist.get('500k_1m', 0) * 3, 150)
                },
                {
                    'label': '£1M-2M',
                    'count': dist.get('1m_2m', 0),
                    'height': min(dist.get('1m_2m', 0) * 3, 150)
                },
                {
                    'label': '£2M+',
                    'count': dist.get('2m_plus', 0),
                    'height': min(dist.get('2m_plus', 0) * 3, 150)
                }
            ]
        
        # Price ranges (horizontal bars)
        if 'price_ranges' in data:
            ranges = data['price_ranges']
            chart_data['price_ranges'] = [
                {
                    'range': '£0-500k',
                    'percentage': ranges.get('0_500k_pct', 0)
                },
                {
                    'range': '£500k-1M',
                    'percentage': ranges.get('500k_1m_pct', 0)
                },
                {
                    'range': '£1M-2M',
                    'percentage': ranges.get('1m_2m_pct', 0)
                },
                {
                    'range': '£2M+',
                    'percentage': ranges.get('2m_plus_pct', 0)
                }
            ]
        
        # Weekly trend line
        if 'weekly_trend' in data:
            trend = data['weekly_trend']
            # Normalize values to 0-180 range for SVG
            max_value = max(trend.values()) if trend else 1
            chart_data['weekly_trend'] = [
                {
                    'label': 'Mon',
                    'value': int((trend.get('monday', 0) / max_value) * 180)
                },
                {
                    'label': 'Wed',
                    'value': int((trend.get('wednesday', 0) / max_value) * 180)
                },
                {
                    'label': 'Fri',
                    'value': int((trend.get('friday', 0) / max_value) * 180)
                },
                {
                    'label': 'Sun',
                    'value': int((trend.get('sunday', 0) / max_value) * 180)
                }
            ]
        
        # Market share (pie chart)
        if 'market_share' in data:
            colors = ['#CBA135', '#B08D57', '#8B7045', '#6B5635', '#4B3C25']
            chart_data['market_share'] = [
                {
                    'name': agency['name'],
                    'percentage': agency['percentage'],
                    'color': colors[i % len(colors)]
                }
                for i, agency in enumerate(data['market_share'])
            ]
        
        # Competitor inventory
        if 'competitor_inventory' in data:
            chart_data['competitor_inventory'] = [
                {
                    'name': comp['name'],
                    'listings': comp['listings']
                }
                for comp in data['competitor_inventory']
            ]
        
        return chart_data
    
    def prepare_opportunities(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Format top opportunities with scoring badges.
        
        Args:
            data: Raw opportunities data
            
        Returns:
            List of formatted opportunity dictionaries
        """
        opportunities = []
        
        if 'top_opportunities' in data:
            for opp in data['top_opportunities']:
                # Determine score class for badge styling
                score = opp.get('score', 0)
                if score >= 80:
                    score_class = 'high'
                elif score >= 60:
                    score_class = 'medium'
                else:
                    score_class = 'low'
                
                opportunities.append({
                    'address': opp.get('address', 'N/A'),
                    'property_type': opp.get('type', 'N/A'),
                    'size': opp.get('size', 0),
                    'price': opp.get('price', 0),
                    'price_per_sqft': opp.get('price_per_sqft', 0),
                    'days_listed': opp.get('days_listed', 0),
                    'score': score,
                    'score_class': score_class
                })
        
        return opportunities
    
    def render_template(self, data: Dict[str, Any]) -> str:
        """
        Render HTML template with data using Jinja2.
        
        Args:
            data: Complete report data
            
        Returns:
            Rendered HTML string
        """
        logger.info("Rendering HTML template")
        
        try:
            template = self.jinja_env.get_template('voxmill_report.html')
            
            # Prepare all data for template
            template_data = {
                'location': data.get('location', 'London, UK'),
                'report_date': data.get('report_date', datetime.now().strftime('%B %Y')),
                'kpis': data.get('kpis', {}),
                'chart_data': self.prepare_chart_data(data),
                'insights': data.get('insights', {}),
                'competitive_analysis': data.get('competitive_analysis', {}),
                'strategic_intelligence': data.get('strategic_intelligence', {}),
                'top_opportunities': self.prepare_opportunities(data)
            }
            
            html_content = template.render(**template_data)
            logger.info("Template rendered successfully")
            
            return html_content
            
        except Exception as e:
            logger.error(f"Error rendering template: {e}")
            raise
    
    def generate_pdf(
        self,
        html_content: str,
        output_filename: str = "Voxmill_Executive_Intelligence.pdf"
    ) -> Path:
        """
        Convert HTML to PDF using WeasyPrint.
        
        Args:
            html_content: Rendered HTML string
            output_filename: Name of output PDF file
            
        Returns:
            Path to generated PDF file
        """
        output_path = self.output_dir / output_filename
        
        logger.info(f"Generating PDF: {output_path}")
        
        try:
            # Load CSS stylesheet
            css_path = self.template_dir / 'voxmill_style.css'
            
            if not css_path.exists():
                logger.warning(f"CSS file not found: {css_path}")
                css = None
            else:
                css = CSS(filename=str(css_path))
                logger.info(f"Loaded CSS from {css_path}")
            
            # Generate PDF
            html = HTML(string=html_content, base_url=str(self.template_dir))
            
            if css:
                html.write_pdf(
                    str(output_path),
                    stylesheets=[css]
                )
            else:
                html.write_pdf(str(output_path))
            
            logger.info(f"✅ PDF generated successfully: {output_path}")
            logger.info(f"📄 File size: {output_path.stat().st_size / 1024:.2f} KB")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Error generating PDF: {e}")
            raise
    
    def generate(
        self,
        output_filename: str = "Voxmill_Executive_Intelligence.pdf"
    ) -> Path:
        """
        Complete pipeline: load data → render template → generate PDF.
        
        Args:
            output_filename: Name of output PDF file
            
        Returns:
            Path to generated PDF file
        """
        logger.info("=" * 70)
        logger.info("VOXMILL MARKET INTELLIGENCE — PDF GENERATION")
        logger.info("=" * 70)
        
        start_time = datetime.now()
        
        try:
            # Load data
            data = self.load_data()
            
            # Render HTML
            html_content = self.render_template(data)
            
            # Generate PDF
            pdf_path = self.generate_pdf(html_content, output_filename)
            
            # Calculate execution time
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info("=" * 70)
            logger.info(f"✅ GENERATION COMPLETE")
            logger.info(f"⏱️  Execution time: {duration:.2f} seconds")
            logger.info(f"📁 Output: {pdf_path}")
            logger.info("=" * 70)
            
            return pdf_path
            
        except Exception as e:
            logger.error("=" * 70)
            logger.error(f"❌ GENERATION FAILED: {e}")
            logger.error("=" * 70)
            raise


def create_sample_data() -> Dict[str, Any]:
    """
    Create sample data for testing (matches expected schema).
    
    Returns:
        Dictionary with sample market intelligence data
    """
    return {
        'location': 'Mayfair · London',
        'report_date': 'November 2025',
        'kpis': {
            'total_properties': 247,
            'property_change': 8.2,
            'avg_price': 2450000,
            'price_change': 3.5,
            'avg_price_per_sqft': 1850,
            'sqft_change': 2.1,
            'days_on_market': 42,
            'velocity_change': -5.3
        },
        'price_distribution': {
            '0_500k': 12,
            '500k_1m': 35,
            '1m_2m': 48,
            '2m_plus': 25
        },
        'price_ranges': {
            '0_500k_pct': 10,
            '500k_1m_pct': 29,
            '1m_2m_pct': 40,
            '2m_plus_pct': 21
        },
        'weekly_trend': {
            'monday': 1750,
            'wednesday': 1820,
            'friday': 1880,
            'sunday': 1850
        },
        'market_share': [
            {'name': 'Knight Frank', 'percentage': 28},
            {'name': 'Savills', 'percentage': 24},
            {'name': 'Strutt & Parker', 'percentage': 18},
            {'name': 'Hamptons', 'percentage': 16},
            {'name': 'Others', 'percentage': 14}
        ],
        'competitor_inventory': [
            {'name': 'Knight Frank', 'listings': 68},
            {'name': 'Savills', 'listings': 59},
            {'name': 'Strutt & Parker', 'listings': 44},
            {'name': 'Hamptons', 'listings': 39},
            {'name': 'Others', 'listings': 37}
        ],
        'insights': {
            'momentum': 'Strong upward momentum with 8.2% increase in active listings, indicating heightened market activity and seller confidence.',
            'positioning': 'Average price positioning at £2.45M reflects premium market segment. Current pricing 3.5% above last week suggests aggressive seller positioning.',
            'velocity': 'Market velocity improving with 5.3% reduction in days on market to 42 days, indicating strong buyer demand and efficient transaction cycles.'
        },
        'competitive_analysis': {
            'summary': 'The Mayfair luxury property market remains highly competitive with Knight Frank and Savills maintaining dominant positions at 28% and 24% market share respectively. The concentration of premium inventory among top-tier agencies indicates a quality-focused competitive landscape.',
            'key_insights': [
                'Knight Frank leads with 68 active listings, representing strong brand positioning in ultra-prime segment',
                'Top 4 agencies control 86% of market share, creating high barriers to entry for new competitors',
                'Average inventory per agency (excluding "Others") is 52.5 listings, suggesting efficient portfolio management',
                'Market fragmentation in "Others" category (14%) indicates opportunities for boutique agencies in niche segments'
            ]
        },
        'strategic_intelligence': {
            'market_dynamics': 'The Mayfair market exhibits classic ultra-prime characteristics with strong demand fundamentals and limited supply dynamics. The 8.2% week-over-week increase in listings suggests a seasonal uptick as sellers position for Q4 transactions. Price appreciation of 3.5% indicates continued wealth concentration in London\'s prime central districts, supported by international buyer interest and limited new development pipeline. The market demonstrates resilience despite broader economic headwinds, with the £2.45M average reflecting sustained premium valuations.',
            'pricing_strategy': 'Current pricing dynamics favor strategic positioning in the £1M-£2M bracket, which captures 40% of market volume and represents the optimal balance between premium positioning and transaction velocity. The £/sqft metric at £1,850 provides competitive differentiation opportunities for properties offering superior specifications or location premiums. Sellers should consider the 42-day average marketing period when pricing, as this indicates a balanced market where both premium pricing and reasonable velocity are achievable with proper positioning.',
            'opportunity_assessment': 'Three primary opportunity vectors emerge: (1) The "Others" category fragmentation suggests consolidation potential for agencies building systematic acquisition capabilities; (2) The 5.3% improvement in market velocity indicates an advantageous environment for properties with compelling value propositions or unique attributes; (3) The concentration of inventory in the £1M-£2M segment creates white space opportunities in adjacent price points, particularly £750K-£1M and £2M-£3M, where competition is less intense and buyer pools remain substantial.',
            'recommendation': 'For clients seeking market entry or portfolio expansion, we recommend focusing acquisition efforts on the £1M-£2M segment with preference for properties offering £/sqft below the £1,850 market average while maintaining premium location and specification standards. This positioning maximizes transaction probability while preserving capital appreciation potential. Consider aggressive marketing timelines within the 42-day velocity window to capitalize on current buyer sentiment and avoid extended marketing periods that may necessitate price adjustments.'
        },
        'top_opportunities': [
            {
                'address': '24 Mount Street, Mayfair',
                'type': 'Apartment',
                'size': 1850,
                'price': 2950000,
                'price_per_sqft': 1595,
                'days_listed': 18,
                'score': 92
            },
            {
                'address': '12 Charles Street, Mayfair',
                'type': 'Townhouse',
                'size': 2400,
                'price': 4200000,
                'price_per_sqft': 1750,
                'days_listed': 25,
                'score': 88
            },
            {
                'address': '7 Grosvenor Square, Mayfair',
                'type': 'Apartment',
                'size': 1600,
                'price': 2400000,
                'price_per_sqft': 1500,
                'days_listed': 31,
                'score': 85
            },
            {
                'address': '15 Curzon Street, Mayfair',
                'type': 'Penthouse',
                'size': 2200,
                'price': 3850000,
                'price_per_sqft': 1750,
                'days_listed': 12,
                'score': 94
            },
            {
                'address': '9 Berkeley Square, Mayfair',
                'type': 'Apartment',
                'size': 1950,
                'price': 3200000,
                'price_per_sqft': 1641,
                'days_listed': 22,
                'score': 89
            },
            {
                'address': '33 Park Lane, Mayfair',
                'type': 'Apartment',
                'size': 1700,
                'price': 2650000,
                'price_per_sqft': 1559,
                'days_listed': 38,
                'score': 81
            },
            {
                'address': '18 South Audley Street',
                'type': 'Townhouse',
                'size': 2800,
                'price': 5100000,
                'price_per_sqft': 1821,
                'days_listed': 45,
                'score': 76
            },
            {
                'address': '5 Hill Street, Mayfair',
                'type': 'Apartment',
                'size': 1550,
                'price': 2350000,
                'price_per_sqft': 1516,
                'days_listed': 28,
                'score': 83
            }
        ]
    }


def main():
    """
    Main execution function for standalone use.
    """
    # Check for custom paths in environment variables
    template_dir = os.getenv('VOXMILL_TEMPLATE_DIR', '/home/claude')
    output_dir = os.getenv('VOXMILL_OUTPUT_DIR', '/tmp')
    data_path = os.getenv('VOXMILL_DATA_PATH', '/tmp/voxmill_analysis.json')
    
    # Initialize generator
    generator = VoxmillPDFGenerator(
        template_dir=template_dir,
        output_dir=output_dir,
        data_path=data_path
    )
    
    # Check if data file exists, create sample if not
    if not Path(data_path).exists():
        logger.warning(f"Data file not found: {data_path}")
        logger.info("Creating sample data for testing...")
        
        sample_data = create_sample_data()
        with open(data_path, 'w', encoding='utf-8') as f:
            json.dump(sample_data, f, indent=2)
        
        logger.info(f"✅ Sample data created: {data_path}")
    
    # Generate PDF
    try:
        pdf_path = generator.generate()
        print(f"\n✅ SUCCESS: PDF generated at {pdf_path}")
        return 0
    except Exception as e:
        print(f"\n❌ ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
