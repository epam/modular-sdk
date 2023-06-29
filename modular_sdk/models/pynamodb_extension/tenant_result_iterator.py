from pynamodb.pagination import ResultIterator


class TenantResultIterator(ResultIterator):

    def __next__(self):
        if self._limit == 0:
            raise StopIteration

        if self._first_iteration:
            self._first_iteration = False
            self._get_next_page()

        while self._index == self._count:
            self._get_next_page()

        item = self._items[self._index]
        self._index += 1
        if self._limit is not None:
            self._limit -= 1

        # fix of the issue of right runtime
        # Tenant type (AwsTenant, etc.) resolving
        cloud = item.get('c').get('S')
        runtime_tenant_type = self._map_fn.get(cloud)
        if runtime_tenant_type.from_raw_data:
            item = runtime_tenant_type.from_raw_data(item)
        return item
