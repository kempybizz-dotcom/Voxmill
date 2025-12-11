"""
VOXMILL BEHAVIORAL CLUSTERING
==============================
Groups agents by trading behavior for advanced pattern recognition

Institutional Use Case:
"Which agents move together?" → "Cluster A (Knight Frank, Savills) moves 85% in sync"
"""

import logging
from typing import Dict, List, Tuple
from collections import defaultdict
import statistics

logger = logging.getLogger(__name__)


def cluster_agents_by_behavior(area: str, agent_profiles: List[Dict]) -> Dict:
    """
    Cluster agents by behavioral similarity
    
    Args:
        area: Market area
        agent_profiles: List of agent behavioral profiles
    
    Returns: Dict with clusters and insights
    """
    
    try:
        if not agent_profiles or len(agent_profiles) < 3:
            return {
                'error': 'insufficient_agents',
                'message': 'Need 3+ agents for clustering'
            }
        
        # Extract behavioral vectors
        agents_data = []
        for profile in agent_profiles:
            vector = extract_behavioral_vector(profile)
            if vector:
                agents_data.append({
                    'agent': profile['agent'],
                    'vector': vector,
                    'profile': profile
                })
        
        if len(agents_data) < 3:
            return {
                'error': 'insufficient_data',
                'message': 'Not enough behavioral data for clustering'
            }
        
        # Perform clustering (simple k-means style)
        clusters = perform_clustering(agents_data, k=3)
        
        # Analyze cluster characteristics
        cluster_analysis = []
        for i, cluster in enumerate(clusters, 1):
            analysis = analyze_cluster(cluster, i)
            cluster_analysis.append(analysis)
        
        # Identify leader-follower relationships
        leader_follower = identify_leader_follower_pairs(clusters)
        
        # Calculate cluster synchronization
        sync_matrix = calculate_synchronization_matrix(clusters)
        
        return {
            'area': area,
            'total_agents': len(agents_data),
            'clusters': cluster_analysis,
            'leader_follower_pairs': leader_follower,
            'synchronization_matrix': sync_matrix,
            'strategic_insights': generate_clustering_insights(cluster_analysis, leader_follower)
        }
        
    except Exception as e:
        logger.error(f"Error clustering agents: {e}", exc_info=True)
        return {
            'error': 'clustering_failed',
            'message': str(e)
        }


def extract_behavioral_vector(profile: Dict) -> List[float]:
    """
    Extract numerical behavioral vector from agent profile
    
    Vector dimensions:
    [0] Aggressiveness (0-1)
    [1] Response speed (0-1, normalized)
    [2] Premium positioning (0-1, normalized)
    [3] Volatility (0-1)
    [4] Consistency (0-1)
    [5] Initiation rate (0-1)
    """
    
    try:
        vector = [
            profile.get('aggressiveness', 0.5),
            min(1.0, profile.get('avg_response_days', 30) / 60),  # Normalize to 0-1
            min(1.0, max(0.0, (profile.get('premium_positioning', 0) + 20) / 40)),  # -20% to +20% → 0-1
            profile.get('volatility', 0.5),
            profile.get('consistency', 0.5),
            profile.get('initiation_rate', 0.5)
        ]
        return vector
    except Exception as e:
        logger.error(f"Error extracting vector: {e}")
        return None


def calculate_distance(vec1: List[float], vec2: List[float]) -> float:
    """Calculate Euclidean distance between two behavioral vectors"""
    
    if len(vec1) != len(vec2):
        return float('inf')
    
    return sum((a - b) ** 2 for a, b in zip(vec1, vec2)) ** 0.5


def perform_clustering(agents_data: List[Dict], k: int = 3) -> List[List[Dict]]:
    """
    Simple k-means clustering
    
    Args:
        agents_data: List of dicts with 'agent', 'vector', 'profile'
        k: Number of clusters
    
    Returns: List of clusters (each cluster is list of agent dicts)
    """
    
    # Initialize centroids (pick k random agents)
    import random
    centroids = [agent['vector'] for agent in random.sample(agents_data, min(k, len(agents_data)))]
    
    # Iterate to convergence (max 10 iterations)
    for iteration in range(10):
        # Assign agents to nearest centroid
        clusters = [[] for _ in range(k)]
        
        for agent in agents_data:
            distances = [calculate_distance(agent['vector'], centroid) for centroid in centroids]
            nearest_cluster = distances.index(min(distances))
            clusters[nearest_cluster].append(agent)
        
        # Remove empty clusters
        clusters = [c for c in clusters if c]
        
        # Recalculate centroids
        new_centroids = []
        for cluster in clusters:
            if cluster:
                vectors = [agent['vector'] for agent in cluster]
                centroid = [statistics.mean(dim) for dim in zip(*vectors)]
                new_centroids.append(centroid)
        
        # Check convergence
        if new_centroids == centroids:
            break
        
        centroids = new_centroids
        k = len(clusters)  # Update k if clusters were removed
    
    return clusters


def analyze_cluster(cluster: List[Dict], cluster_id: int) -> Dict:
    """Analyze characteristics of a cluster"""
    
    agents = [agent['agent'] for agent in cluster]
    profiles = [agent['profile'] for agent in cluster]
    vectors = [agent['vector'] for agent in cluster]
    
    # Calculate cluster centroid (average characteristics)
    centroid = [statistics.mean(dim) for dim in zip(*vectors)]
    
    # Interpret centroid dimensions
    aggressiveness = centroid[0]
    response_speed_normalized = centroid[1]
    premium_positioning_normalized = centroid[2]
    volatility = centroid[3]
    consistency = centroid[4]
    initiation_rate = centroid[5]
    
    # Denormalize for reporting
    avg_response_days = response_speed_normalized * 60
    premium_positioning = (premium_positioning_normalized * 40) - 20
    
    # Determine cluster archetype
    if aggressiveness > 0.7 and initiation_rate > 0.6:
        archetype = 'Market Leaders'
        description = 'High aggression, frequent initiators, set market direction'
    elif aggressiveness < 0.3 and response_speed_normalized > 0.7:
        archetype = 'Conservative Followers'
        description = 'Low aggression, slow responders, risk-averse positioning'
    elif consistency > 0.7 and volatility < 0.3:
        archetype = 'Stable Operators'
        description = 'High consistency, low volatility, predictable behavior'
    elif volatility > 0.6:
        archetype = 'Tactical Opportunists'
        description = 'High volatility, adaptive strategy, condition-dependent'
    else:
        archetype = 'Balanced Movers'
        description = 'Moderate across dimensions, market-neutral positioning'
    
    # Calculate cluster cohesion
    intra_cluster_distances = []
    for i, agent1 in enumerate(cluster):
        for agent2 in cluster[i+1:]:
            dist = calculate_distance(agent1['vector'], agent2['vector'])
            intra_cluster_distances.append(dist)
    
    cohesion = 1 - (statistics.mean(intra_cluster_distances) if intra_cluster_distances else 0)
    cohesion = max(0, min(1, cohesion))
    
    return {
        'cluster_id': cluster_id,
        'archetype': archetype,
        'description': description,
        'agents': agents,
        'agent_count': len(agents),
        'cohesion': round(cohesion, 2),
        'characteristics': {
            'aggressiveness': round(aggressiveness, 2),
            'avg_response_days': round(avg_response_days, 1),
            'premium_positioning_pct': round(premium_positioning, 1),
            'volatility': round(volatility, 2),
            'consistency': round(consistency, 2),
            'initiation_rate': round(initiation_rate, 2)
        }
    }


def identify_leader_follower_pairs(clusters: List[List[Dict]]) -> List[Dict]:
    """
    Identify leader-follower relationships across clusters
    
    Leaders = High initiation rate + High consistency
    Followers = Low initiation rate + High response correlation
    """
    
    pairs = []
    
    # Find potential leaders (high initiation, high consistency)
    all_agents = []
    for cluster in clusters:
        all_agents.extend(cluster)
    
    leaders = []
    followers = []
    
    for agent_data in all_agents:
        profile = agent_data['profile']
        initiation = profile.get('initiation_rate', 0)
        consistency = profile.get('consistency', 0)
        
        if initiation > 0.6 and consistency > 0.7:
            leaders.append(agent_data)
        elif initiation < 0.4:
            followers.append(agent_data)
    
    # Match leaders to followers based on vector similarity
    for leader in leaders:
        for follower in followers:
            distance = calculate_distance(leader['vector'], follower['vector'])
            
            # If similar behavior (except initiation), likely follower
            if distance < 0.5:  # Threshold for similarity
                correlation_strength = 1 - distance
                
                pairs.append({
                    'leader': leader['agent'],
                    'follower': follower['agent'],
                    'correlation': round(correlation_strength, 2),
                    'confidence': 0.75 if correlation_strength > 0.7 else 0.6,
                    'pattern': 'Leader initiates, follower responds with similar magnitude',
                    'avg_lag_days': int(abs(
                        leader['profile'].get('avg_response_days', 20) - 
                        follower['profile'].get('avg_response_days', 30)
                    ))
                })
    
    # Sort by correlation strength
    pairs.sort(key=lambda x: x['correlation'], reverse=True)
    
    return pairs[:10]  # Top 10 pairs


def calculate_synchronization_matrix(clusters: List[List[Dict]]) -> Dict:
    """
    Calculate how synchronized clusters are with each other
    
    High sync = clusters move together
    Low sync = independent movement
    """
    
    matrix = {}
    
    for i, cluster1 in enumerate(clusters):
        for j, cluster2 in enumerate(clusters):
            if i >= j:
                continue
            
            # Calculate average distance between all agent pairs
            distances = []
            for agent1 in cluster1:
                for agent2 in cluster2:
                    dist = calculate_distance(agent1['vector'], agent2['vector'])
                    distances.append(dist)
            
            avg_distance = statistics.mean(distances) if distances else 1.0
            synchronization = max(0, 1 - avg_distance)
            
            cluster1_id = cluster1[0]['profile'].get('agent', f'Cluster{i+1}')
            cluster2_id = cluster2[0]['profile'].get('agent', f'Cluster{j+1}')
            
            matrix[f"Cluster {i+1} ↔ Cluster {j+1}"] = {
                'synchronization': round(synchronization, 2),
                'interpretation': (
                    'High synchronization - Move together' if synchronization > 0.7 else
                    'Moderate synchronization - Some correlation' if synchronization > 0.4 else
                    'Low synchronization - Independent movement'
                )
            }
    
    return matrix


def generate_clustering_insights(cluster_analysis: List[Dict], leader_follower: List[Dict]) -> List[str]:
    """Generate strategic insights from clustering"""
    
    insights = []
    
    # Insight 1: Dominant cluster
    largest_cluster = max(cluster_analysis, key=lambda c: c['agent_count'])
    insights.append(
        f"Dominant behavior: {largest_cluster['archetype']} "
        f"({largest_cluster['agent_count']} agents, {largest_cluster['cohesion']*100:.0f}% cohesion)"
    )
    
    # Insight 2: Leader-follower dynamics
    if leader_follower:
        top_pair = leader_follower[0]
        insights.append(
            f"Strongest leader-follower: {top_pair['leader']} → {top_pair['follower']} "
            f"({top_pair['correlation']*100:.0f}% correlation, {top_pair['avg_lag_days']}d lag)"
        )
    
    # Insight 3: Market fragmentation
    if len(cluster_analysis) >= 3:
        insights.append(
            f"Market fragmented: {len(cluster_analysis)} distinct behavioral clusters detected"
        )
    else:
        insights.append(
            f"Market consolidated: {len(cluster_analysis)} behavioral clusters suggest coordinated movement"
        )
    
    # Insight 4: Predictability
    avg_consistency = statistics.mean([c['characteristics']['consistency'] for c in cluster_analysis])
    if avg_consistency > 0.7:
        insights.append(f"High predictability: {avg_consistency*100:.0f}% average consistency across clusters")
    else:
        insights.append(f"Low predictability: {avg_consistency*100:.0f}% average consistency suggests volatile conditions")
    
    return insights
