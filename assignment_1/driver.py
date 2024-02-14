def dj(customer, product_model):
    return purchase_data_df.groupBy(customer).agg(collect_set(product_model)).filter(expr("collect_set(product_model) = array('A')"))


dj('customer')