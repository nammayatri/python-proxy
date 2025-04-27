import polyline
import math 
from typing import List

def calculate_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    using the haversine formula
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Radius of earth in kilometers
    r = 6371
    
    return c * r


def is_point_near_polyline(point_lat, point_lon, polyline_points, max_distance_meter=50):
    """
    Simpler function to check if a point is within max_distance_meter of any 
    segment of the polyline.
    """
    if not polyline_points or len(polyline_points) < 2:
        return False, float('inf'), None
        
    min_distance = float('inf')
    
    min_segment = None
    
    # Check each segment of the polyline
    for i in range(len(polyline_points) - 1):
        # Start and end points of current segment
        p1_lat, p1_lon = polyline_points[i]
        p2_lat, p2_lon = polyline_points[i + 1]
        
        # Calculate distance to this segment using a simple approximation
        # For short segments, this is reasonable and much simpler
        
        # Calculate distances to segment endpoints
        d1 = calculate_distance(point_lat, point_lon, p1_lat, p1_lon)
        d2 = calculate_distance(point_lat, point_lon, p2_lat, p2_lon)
        
        # Calculate length of segment
        segment_length = calculate_distance(p1_lat, p1_lon, p2_lat, p2_lon)
        
        # Use the simplified distance formula (works well for short segments)
        if segment_length > 0:
            # Projection calculation
            # Vector from p1 to p2
            v1x = p2_lon - p1_lon
            v1y = p2_lat - p1_lat
            
            # Vector from p1 to point
            v2x = point_lon - p1_lon
            v2y = point_lat - p1_lat
            
            # Dot product
            dot = v1x * v2x + v1y * v2y
            
            # Squared length of segment
            len_sq = v1x * v1x + v1y * v1y
            
            # Projection parameter (t)
            t = max(0, min(1, dot / len_sq))
            
            # Projected point
            proj_x = p1_lon + t * v1x
            proj_y = p1_lat + t * v1y
            
            # Distance to projection
            distance = calculate_distance(point_lat, point_lon, proj_y, proj_x)
        else:
            # If segment is very short, just use distance to p1
            distance = d1
            
        # Update minimum distance
        if distance < min_distance:
            min_segment = i
            min_distance = distance
            
    # Check if within threshold (convert meters to kilometers)
    max_distance_km = max_distance_meter / 1000
    return min_distance <= max_distance_km, min_distance, min_segment

    # Use the library's implementation when available
def decode_polyline(polyline_str):
    """Wrapper for polyline library's decoder"""
    if not polyline_str:
        return []
    try:
        return polyline.decode(polyline_str)
    except Exception as e:
        print(f"Error decoding polyline: {e}")
        return []


def calculate_route_match_score(vehicle_points: List[dict], max_distance_meter: float = 100) -> float:
    """
    Calculate how well a route matches a series of vehicle_points, considering direction.
    Uses polyline for more accurate route matching when available.
    Returns a score between 0 and 1, where 1 is a perfect match.
    """
    try:
        # Check if stops is a dict with polyline and stops keys
        # Using a polyline string that represents a route
        # This is a properly escaped polyline string to avoid Unicode errors
        route_polyline = r"_swnAicphNgArB[WIGDMh@_AvCyFfAuBtCsEl@_A`@{@n@mAd@{@f@i@z@s@rCyBlA}@r@_@|@e@bA[pAW~@IjBGxAErBAhBBvBBvJPbGFhQLvAGz@I~Dq@fC_@vASt@GlA@nADz@B~A@jBDrBC^C`EUbDSfEIhEErA?xPAhHAdEDxDCzEBdGBfCHlF^l@PbBNnABBDDFxEd@w@jKz@NhATbMfCZFtEz@|@V|@d@hAj@bClAtBbAp@TnHfAhIdAr@Fn@@xF@tDDd@BlANvEdAhEhAhCn@fCf@dFnAB@`HjBvAZt@HbD\lBNdABhC?dECdDMxABrB^pAd@dAp@h@n@d@n@Xf@Xt@h@bAp@r@VTLFd@Lh@\~AZLBzCb@vB\ZJ|@^RL^Zl@`AT~@Dx@Ab@EbASrBSfDCr@Dt@Pt@j@bAl@|@vBvCxFrHnBpCdB|Bv@dAvDjFtCnDlBtBdAjBvAjD|@|BhBhErAlDlAjCrBtEjAxBb@t@jAlBpElGfExFjBlCtAdB`D|ExDjGRZtCrEp@nALRbAhBvE|HfCzEhInPpChFbBfDd@~@nAdDZhAj@pCp@tDNj@^~@t@jAHIIH`A|A`FbIlAjBjArBtAbCpBtDnCvEv@pAh@|@Z\`HpLJPpC~Er@`An@p@n@b@rAj@rIdDtBr@b@PvBr@jC`Ap@Z`AVrCdAxDbBhF`CfCfAvDfBrIzDvB~@dEbB|GxCjGlCjIpDnEnBbChA|@d@|BvAzB|AtJ`HnC|BrCpBjGxD~JjHxBzAdAx@hA|@zCtCxC|ChFnFb@b@jAz@\^d@l@v@r@|BzBdFjEzBhB`Az@`FbEzF|EjC`CvFvFnEnErJvJjChChCpClDnDzJzJ~F|FpCzCrBhBHJ`BfBpEfErA|AjAjAxC~C|@fAnBlC`BxBr@bArDbFh@t@|@hBp@fBxA~CjBrE`CpFjA~B`@p@dBhC`Ax@v@NtAJtFPlBTfAVj@Rf@Xr@j@\d@b@z@~@jCj@zAd@|@h@x@`AhA^^`Av@xAfAvBzArBxApClB|E|CfDxBr@j@fB`Bv@n@xBzAvE~C~XvRxFzDp@d@N?J@PHdL~Hl@ZJL`@Tj@`@pBfA~B|@`A^XT|GxBlW~HlChA|GnBpIfClDbAzHxBxS|FpF~AnCx@xDfAb\fJ`HpBO`@sGkBeGeBu@SkA_@aD}@iJmCaD}@_EiAiAc@yAc@_@S[FaD_Ai@Oe@EwASmBi@sA]{DuAUI^}@dEhAtMrDlA^`Ct@`Cv@"
        polyline_points = decode_polyline(route_polyline)
        min_points_required = 4
        if not vehicle_points or len(vehicle_points) < min_points_required:
            print("here")
            return 0.0

        # Sort vehicle_points by timestamp to ensure they're in chronological order
        vehicle_points = sorted(vehicle_points, key=lambda x: x.get('timestamp', 0))
    
        if polyline_points:
            # Count how many vehicle_points are near the polyline
            near_points = []
            total_distance = 0.0
            
            max_distance_km = max_distance_meter / 1000
            min_segments_list = []
            for point in vehicle_points:
                try:
                    is_near, distance, min_segment_start = is_point_near_polyline(
                        point['lat'], point['lon'], polyline_points, max_distance_meter
                    )
                    if is_near:
                        if min_segment_start is not None:
                            min_segments_list.append(min_segment_start)
                        near_points.append(point)
                        total_distance += distance
                except (KeyError, ValueError, TypeError) as e:
                    print(f"Error checking if point is near polyline: {e}, point: {point}")
                    continue
            
            # Calculate proximity score (0-1)
            proximity_ratio = len(near_points) / len(vehicle_points) if len(vehicle_points) > 0 else 0
            
            # Only proceed if enough vehicle_points are near the polyline
            print("proximity_ratio", proximity_ratio)
            if proximity_ratio >= 0.3:
                # Convert set to list and sort to check direction
                print("min_segments_list", min_segments_list)
                if len(min_segments_list) >= 2 and min_segments_list[0] < min_segments_list[-1]:
                    print(f"Route Id: {len(near_points)}/{len(vehicle_points)}, Score: {proximity_ratio:.2f}")
                    return proximity_ratio
            return 0.0
    except Exception as e:
        print(f"Error calculating route match score: {e}\nTraceback")
        return 0.0
    

kk = [{"lat": 12.891449183333334, "lon": 80.08525093333333, "timestamp": 1745703546}, {"lat": 12.890673566666667, "lon": 80.0849164, "timestamp": 1745703556}, {"lat": 12.889637733333334, "lon": 80.08450243333333, "timestamp": 1745703566}, {"lat": 12.888765416666667, "lon": 80.08416348333333, "timestamp": 1745703576}, {"lat": 12.887541233333334, "lon": 80.08367423333333, "timestamp": 1745703586}, {"lat": 12.886612283333333, "lon": 80.083242, "timestamp": 1745703596}, {"lat": 12.885623616666667, "lon": 80.0828444, "timestamp": 1745703606}, {"lat": 12.8846471, "lon": 80.08243251666667, "timestamp": 1745703616}, {"lat": 12.88336835, "lon": 80.0820538, "timestamp": 1745703626}, {"lat": 12.88238265, "lon": 80.08171246666667, "timestamp": 1745703636}, {"lat": 12.88114425, "lon": 80.08117251666667, "timestamp": 1745703646}, {"lat": 12.880340366666667, "lon": 80.08076618333334, "timestamp": 1745703656}, {"lat": 12.879453116666667, "lon": 80.08039928333334, "timestamp": 1745703666}, {"lat": 12.878595266666666, "lon": 80.08011638333333, "timestamp": 1745703676}, {"lat": 12.87755565, "lon": 80.07967858333333, "timestamp": 1745703686}, {"lat": 12.876733716666667, "lon": 80.07938676666667, "timestamp": 1745703696}, {"lat": 12.875526833333334, "lon": 80.07892975, "timestamp": 1745703706}, {"lat": 12.8751138, "lon": 80.07877636666667, "timestamp": 1745703716}, {"lat": 12.87472445, "lon": 80.07860145, "timestamp": 1745703726}, {"lat": 12.874194616666667, "lon": 80.07839868333333, "timestamp": 1745703736}, {"lat": 12.873995566666666, "lon": 80.0786958, "timestamp": 1745703746}, {"lat": 12.87379615, "lon": 80.0792668, "timestamp": 1745703766}, {"lat": 12.873713383333333, "lon": 80.0793888, "timestamp": 1745703786}, {"lat": 12.873587133333332, "lon": 80.07970845, "timestamp": 1745703796}, {"lat": 12.8733892, "lon": 80.07971235, "timestamp": 1745703806}]
# kk = kk[::-1]
print(calculate_route_match_score(kk))

