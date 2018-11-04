
NODE_DESCRIPTIONS = {
    'Seq Scan': 'Scans the entire relation as stored on disk.',
    'Index Scan': 'Performs a B-tree traversal, walks through the leaf nodes to find all matching entries, and fetches the corresponding table data.',
    'Hash Join': 'Loads the candidate records from one side of the join into a hash table which is then probed for each record from the other side of the join.',
    'Aggregate': 'Groups records together based on a key or an aggregate function.',
    'Limit': 'Returns a specified number of rows from a record set.',
    'Sort': 'Sorts a record set based on the specified sort key.',
    'Nested Loop': 'Merges two record sets by looping through every record in the first set and trying to find a match in the second set.',
    'Merge Join': 'Merges two record sets by first sorting them on a join key.',
    'Hash': 'Generates a hash table from the records in the input recordset.',
    'CTE_Scan': 'Performs sequential scan of a Common Table Expression (CTE) query results.',
    'Bitmap Heap Scan': 'Searches through the pages returned by the Bitmap Index Scan for relevant rows.',
    'Index Only Scan': 'Finds relevant records based on an Index. Performs a single read operation from the index and does not read from the corresponding table.'
}

def analyze(execution_plan):

    root_plan = execution_plan['Plan']
    analyze_plan(root_plan)

    execution_plan['Longest Duration'] = get_longest_duration(root_plan)
    find_slowest_node(root_plan, execution_plan['Longest Duration'])

    execution_plan['Highest Cost'] = get_longest_duration(root_plan)
    find_costliest_node(root_plan, execution_plan['Highest Cost'])

    execution_plan['Greatest Errors'] = get_greatest_errors(root_plan)
    find_greatest_errors_node(root_plan, execution_plan['Greatest Errors'])

    execution_plan['Largest Size'] = get_largest_nodes(root_plan)
    find_largest_node(root_plan, execution_plan['Largest Size'])

    return execution_plan


def analyze_plan(plan):
    if 'Plans' in plan.keys():
        for sub_plan in plan['Plans']:
            analyze_plan(sub_plan)

    plan['Actual Duration'] = calculate_actual_duration(plan)
    plan['Actual Cost'] = calculate_actual_cost(plan)
    plan['Estimate Errors'] = calculate_estimate_errors(plan)
    plan['Description'] = get_node_description(plan)

def get_node_description(plan):

    description = ''
    if plan['Node Type'] in NODE_DESCRIPTIONS:
        description = NODE_DESCRIPTIONS[plan['Node Type']]

    return description

def calculate_actual_duration(plan):
    actual_duration = plan['Actual Total Time']
    if 'Plans' in plan.keys():
        for sub_plan in plan['Plans']:
            # since CTE scan duration is already included in its subnodes,
            # it should be be subtracted from the duration of this node
            if (sub_plan['Node Type'] != 'CTE_Scan'):
                actual_duration -= sub_plan['Actual Total Time'];

    # time is reported for an invidual loop
    # actual duration must be adjusted by number of loops
    actual_duration = actual_duration * plan['Actual Loops'];
    return actual_duration;

def get_longest_duration(plan):
    duration = plan['Actual Duration']

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            duration = max(duration, get_longest_duration(sub_plan))
        return duration
    return duration

def find_slowest_node(plan, longest_duration):
    plan['Is Slowest'] = False
    if plan['Actual Duration'] == longest_duration:
        plan['Is Slowest'] = True

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            find_slowest_node(sub_plan, longest_duration)

def calculate_actual_cost(plan):
    actual_cost = plan['Total Cost']
    if 'Plans' in plan.keys():
        for sub_plan in plan['Plans']:
            # since CTE scan duration is already included in its subnodes,
            # it should be be subtracted from the duration of this node
            if (sub_plan['Node Type'] != 'CTE_Scan'):
                actual_cost -= sub_plan['Total Cost'];

    # time is reported for an invidual loop
    # actual duration must be adjusted by number of loops
    actual_cost = actual_cost * plan['Actual Loops'];
    return actual_cost;

def get_highest_cost(plan):
    cost = plan['Actual Cost']

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            cost = max(cost, get_highest_cost(sub_plan))
        return cost
    return cost

def find_costliest_node(plan, highest_cost):
    plan['Is Costliest'] = False
    if plan['Actual Cost'] == highest_cost:
        plan['Is Costliest'] = True

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            find_costliest_node(sub_plan, highest_cost)

def calculate_estimate_errors(plan):
    return abs(plan['Actual Rows'] - plan['Plan Rows'])

def get_greatest_errors(plan):
    error = plan['Estimate Errors']

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            error = max(error, get_greatest_errors(sub_plan))
        return error
    return error

def find_greatest_errors_node(plan, greatest_errors):
    plan['Has Greatest Errors'] = False
    if greatest_errors > 0 and plan['Estimate Errors'] == greatest_errors:
        plan['Has Greatest Errors'] = True

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            find_greatest_errors_node(sub_plan, greatest_errors)

def get_largest_nodes(plan):
    size = plan['Actual Rows']

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            size = max(size, get_largest_nodes(sub_plan))
        return size
    return size

def find_largest_node(plan, largest_size):
    plan['Is Largest'] = False
    if plan['Actual Rows'] == largest_size:
        plan['Is Largest'] = True

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            find_largest_node(sub_plan, largest_size)
