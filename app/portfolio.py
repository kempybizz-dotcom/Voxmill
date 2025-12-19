# ========================================
        # PORTFOLIO TRACKING COMMANDS
        # ========================================

        portfolio_keywords = ['my portfolio', 'my properties', 'add property', 'portfolio summary']

        if any(kw in message_lower for kw in portfolio_keywords):
            from app.portfolio import get_portfolio_summary, add_property_to_portfolio, parse_property_from_message
            
            if 'add property' in message_lower:
                # Check if user is sending property details
                parsed_property = parse_property_from_message(message_text)
                
                if parsed_property:
                    # Add property
                    response = add_property_to_portfolio(sender, parsed_property)
                    await send_twilio_message(sender, response)
                    
                    # Update conversation session
                    conversation = ConversationSession(sender)
                    conversation.update_session(
                        user_message=message_text,
                        assistant_response=response,
                        metadata={'category': 'portfolio_add'}
                    )
                    
                    return
                else:
                    # Guide user through adding property
                    response = """ADD PROPERTY TO PORTFOLIO

Reply with property details:

Format: "Property: [address], Purchase: £[amount], Date: [YYYY-MM-DD], Region: [region]"

Example: "Property: 123 Park Lane, Purchase: £2500000, Date: 2023-01-15, Region: Mayfair" """
                    
                    await send_twilio_message(sender, response)
                    return
            
            # Show portfolio summary
            portfolio = get_portfolio_summary(sender)
            
            if portfolio.get('error'):
                response = """No properties in portfolio.

Add a property: "Add property [details]" """
                await send_twilio_message(sender, response)
                return
            
            # Format portfolio response
            prop_list = []
            for prop in portfolio['properties'][:5]:
                prop_list.append(
                    f"• {prop['address']}: £{prop['current_estimate']:,.0f} "
                    f"({prop['gain_loss_pct']:+.1f}%)"
                )
            
            response = f"""PORTFOLIO SUMMARY

{chr(10).join(prop_list)}

Total Value: £{portfolio['total_current_value']:,.0f}
Total Gain/Loss: £{portfolio['total_gain_loss']:,.0f} ({portfolio['total_gain_loss_pct']:+.1f}%)

Properties: {portfolio['property_count']}"""
            
            await send_twilio_message(sender, response)
            
            # Update conversation session
            conversation = ConversationSession(sender)
            conversation.update_session(
                user_message=message_text,
                assistant_response=response,
                metadata={'category': 'portfolio_summary'}
            )
            
            return
