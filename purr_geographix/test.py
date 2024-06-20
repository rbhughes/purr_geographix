import numpy as np
from scipy.spatial import KDTree
from collections import deque


# Example usage:
# points = np.array([
#     [0, 0], [1, 0], [1, 1], [0, 1], [0.5, 0.5],
#     [0.5, 0.75], [0.75, 0.5], [0.25, 0.25], [0.25, 0.75]
# ])
#
# hull_points = find_concave_hull(points, k=3)
# print(hull_points)
def k_nearest_neighbors(points, k):
    tree = KDTree(points)
    neighbors = tree.query(points, k+1)[1][:, 1:]
    return neighbors

def find_concave_hull(points, k):
    if len(points) < 3:
        raise ValueError("At least 3 points are required")
    if k < 3:
        k = 3

    points = np.array(points)
    neighbors = k_nearest_neighbors(points, k)
    start = np.argmin(points[:, 1])

    hull = deque()
    hull.append(start)

    current_point = start
    previous_angle = 0
    used_points = set()
    used_points.add(start)

    while True:
        current_neighbors = neighbors[current_point]
        angles = []
        for neighbor in current_neighbors:
            if neighbor not in used_points:
                dx = points[neighbor][0] - points[current_point][0]
                dy = points[neighbor][1] - points[current_point][1]
                angle = np.arctan2(dy, dx)
                angle = (angle - previous_angle + 2 * np.pi) % (2 * np.pi)
                angles.append((angle, neighbor))

        if not angles:
            # If no valid angles were found, break the loop to prevent infinite loops
            break

        angles.sort()
        next_point = angles[0][1]

        if next_point == start:
            break

        hull.append(next_point)
        previous_angle = np.arctan2(points[next_point][1] - points[current_point][1],
                                    points[next_point][0] - points[current_point][0])
        current_point = next_point
        used_points.add(next_point)

    hull_points = points[list(hull)]

    return hull_points


points = np.array([[-97.92775,  27.64055],
 [-97.91639,  27.63778],
 [-97.91694,  27.62991],
 [-97.92,     27.62993],
 [-97.9193,   27.63234],
 [-97.92866,  27.62921],
 [-97.91179, 27.63958],
 [-97.91181, 27.6364],
 [-97.92284, 27.63826],
 [-97.91115, 27.63186],
 [-97.92286, 27.63433],
 [-97.93144, 27.63451],
 [-97.92508, 27.63116],
 [-97.90777, 27.62867],
 [-97.91746, 27.62674],
 [-97.92708, 27.63888],
 [-97.93197, 27.63073],
 [-97.92881, 27.63405],
 [-97.91186, 27.62672],
 [-97.91944, 27.63764],
 [-97.9313, 27.6268],])

hull_points = find_concave_hull(points, k=6)
print(hull_points)

# print(points)
