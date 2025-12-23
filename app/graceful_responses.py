# ========================================
        # SOFT REGION SWITCHING
        # ========================================
        
        # Check if user is confirming a region switch
        if message_lower in ['yes', 'yeah', 'yep', 'sure', 'ok', 'okay', 'switch']:
            # Check if last response was a region switch offer
            last_metadata = safe_get_last_metadata(conversation)
            
            if last_metadata.get('offered_region_switch'):
                new_region = last_metadata.get('offered_region')
                
                # Update preferred region
                client_profile['preferences']['preferred_regions'] = [new_region]
                
                # Update in MongoDB
                from pymongo import MongoClient
                MONGODB_URI = os.getenv('MONGODB_URI')
                if MONGODB_URI:
                    mongo_client = MongoClient(MONGODB_URI)
                    db = mongo_client['Voxmill']
                    db['client_profiles'].update_one(
                        {'whatsapp_number': sender},
                        {'$set': {'preferences.preferred_regions': [new_region]}}
                    )
                
                await send_twilio_message(
                    sender,
                    f"Region switched to {new_region}.\n\nStanding by."
                )
                
                # Update session
                conversation.update_session(
                    user_message=message_text,
                    assistant_response=f"Region switched to {new_region}.",
                    metadata={'category': 'region_switch_confirmed'}
                )
                
                return
