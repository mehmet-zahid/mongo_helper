
# All the operators are defined here of the mongo query language
class Operators:
    @staticmethod
    def eq(field, value):
        return {field: {"$eq": value}}

    @staticmethod
    def ne(field, value):
        return {field: {"$ne": value}}

    @staticmethod
    def gt(field, value):
        return {field: {"$gt": value}}

    @staticmethod
    def gte(field, value):
        return {field: {"$gte": value}}

    @staticmethod
    def lt(field, value):
        return {field: {"$lt": value}}

    @staticmethod
    def lte(field, value):
        return {field: {"$lte": value}}

    @staticmethod
    def in_(field, values):
        return {field: {"$in": values}}

    @staticmethod
    def nin(field, values):
        return {field: {"$nin": values}}

    @staticmethod
    def and_(*conditions):
        return {"$and": [*conditions]}

    @staticmethod
    def regex(field, pattern, options=""):
        return {field: {"$regex": pattern, "$options": options}}

    @staticmethod
    def or_(*conditions):
        return {"$or": [*conditions]}

    @staticmethod
    def not_(condition):
        return {"$not": condition}

    @staticmethod
    def set(field, value):
        return {"$set": {field: value}}

    @staticmethod
    def exists(field, exists=True):
        return {field: {"$exists": exists}}

    @staticmethod
    def elemMatch(field, condition):
        return {field: {"$elemMatch": condition}}

    @staticmethod
    def all(field, values):
        return {field: {"$all": values}}

    @staticmethod
    def size(field, size):
        return {field: {"$size": size}}

    @staticmethod
    def type(field, type_):
        return {field: {"$type": type_}}

    @staticmethod
    def text(search):
        return {"$text": {"$search": search}}

    @staticmethod
    def geoWithin(field, geometry):
        return {field: {"$geoWithin": {"$geometry": geometry}}}

    @staticmethod
    def geoIntersects(field, geometry):
        return {field: {"$geoIntersects": {"$geometry": geometry}}}

    @staticmethod
    def near(field, point, max_distance=None, min_distance=None):
        query = {field: {"$near": {"$geometry": point}}}
        if max_distance is not None:
            query[field]["$near"]["$maxDistance"] = max_distance
        if min_distance is not None:
            query[field]["$near"]["$minDistance"] = min_distance
        return query

    @staticmethod
    def nearSphere(field, point, max_distance=None, min_distance=None):
        query = {field: {"$nearSphere": {"$geometry": point}}}
        if max_distance is not None:
            query[field]["$nearSphere"]["$maxDistance"] = max_distance
        if min_distance is not None:
            query[field]["$nearSphere"]["$minDistance"] = min_distance
        return query

    @staticmethod
    def geoNear(field, point, distance_field, max_distance=None, min_distance=None, spherical=False):
        query = {
            "$geoNear": {
                "near": point,
                "distanceField": distance_field,
                "spherical": spherical
            }
        }
        if max_distance is not None:
            query["$geoNear"]["maxDistance"] = max_distance
        if min_distance is not None:
            query["$geoNear"]["minDistance"] = min_distance
        return query

    @staticmethod
    def mod(field, divisor, remainder):
        return {field: {"$mod": [divisor, remainder]}}

    @staticmethod
    def where(code):
        return {"$where": code}

    @staticmethod
    def expr(expression):
        return {"$expr": expression}

    @staticmethod
    def jsonSchema(schema):
        return {"$jsonSchema": schema}

    @staticmethod
    def search(search_field, pattern):
        projection = {
            '$search': {
                'index': 'default',
                'text': {
                    'query': f'${search_field}',
                    'path': 'name'
                }
            }
        }
        return projection

    @staticmethod
    def search_text(search_field, pattern):
        projection = {
            '$search': {
                'index': 'default',
                'text': {
                    'query': f'${search_field}',
                    'path': 'name'
                }
            }
        }
        return projection

    @staticmethod
    def search_text_score(search_field, pattern):
        projection = {
            '$search': {
                'index': 'default',
                'text': {
                    'query': f'${search_field}',
                    'path': 'name'
                }
            }
        }
        return projection

    @staticmethod
    def search_meta(search_field, pattern):
        projection = {
            '$search': {
                'index': 'default',
                'text': {
                    'query': f'${search_field}',
                    'path': 'name'
                }
            }
        }
        return projection

    @staticmethod
    def search_meta_score(search_field, pattern):
        projection = {
            '$search': {
                'index': 'default',
                'text': {
                    'query': f'${search_field}',
                    'path': 'name'
                }
            }
        }
        return projection

    @staticmethod
    def search_highlight(search_field, pattern):
        projection = {
            '$search': {
                'index': 'default',
                'text': {
                    'query': f'${search_field}',
                    'path': 'name'
                }
            }
        }
        return projection

    @staticmethod
    def search_highlight_score(search_field, pattern):
        projection = {
            '$search': {
                'index': 'default',
                'text': {
                    'query': f'${search_field}',
                    'path': 'name'
                }
            }
        }
        return projection

    @staticmethod
    def search_highlight_meta(search_field, pattern):
        projection = {
            '$search': {
                'index': 'default',
                'text': {
                    'query': f'${search_field}',
                    'path': 'name'
                }
            }
        }
        return projection

    @staticmethod
    def search_highlight_meta_score(search_field, pattern):
        projection = {
            '$search': {
                'index': 'default',
                'text': {
                    'query': f'${search_field}',
                    'path': 'name'
                }
            }
        }
        return projection

    @staticmethod
    def search_highlight_text(search_field, pattern):
        projection = {
            '$search': {
                'index': 'default',
                'text': {
                    'query': f'${search_field}',
                    'path': 'name'
                }
            }
        }
        return projection

    @staticmethod
    def search_highlight_text_score(search_field, pattern):
        projection = {
            '$search': {
                'index': 'default',
                'text': {
                    'query': f'${search_field}',
                    'path': 'name'
                }
            }
        }
        return projection
