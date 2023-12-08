
def singleton(cls):
    instance = [None]
    def wrapper(*args, **kwargs):
        if instance[0] is None:
            instance[0] = cls(*args, **kwargs)
        return instance[0]
    return wrapper

@staticmethod
def select_fields(include=None, exclude=None):
    if include and exclude:
        raise ValueError("Cannot include and exclude fields at the same time.")
    projection = {}
    if include:
        for field in include:
            projection[field] = 1
    elif exclude:
        for field in exclude:
            projection[field] = 0
    return projection

@staticmethod
def select_with_regex(search_field, pattern):
    """
    seach_field: The field to search for for the pattern --> 'sitemap.SITEMAP_BRAND_LINKS'
    """
    projection ={
        f'{search_field}': {
            '$filter': {
                'input': f'${search_field}',
                'cond': {
                    '$regexMatch': {
                        'input': '$$this',
                        'regex': pattern
            }}}}}
    return projection

async def create_index(self, keys: Union[str, List[Tuple[str, int]]], options: Optional[Dict[str, Any]] = None) -> str:
    if isinstance(keys, str):
        field_name = self.model.__fields__[keys].alias
        keys = [(field_name, 1)]
    if options is None:
        options = {}
    return await self.collection.create_index(keys, **options)

async def create_multi_indexes(self, indexes: List[Tuple[Union[str, List[Tuple[str, int]]], Dict[str, Any]]]) -> List[str]:
    index_models = []
    for keys, options in indexes:
        if isinstance(keys, str):
            keys = [(keys, 1)]
        index_model = IndexModel(keys, **options)
        index_models.append(index_model)
    return await self.collection.create_indexes(index_models)