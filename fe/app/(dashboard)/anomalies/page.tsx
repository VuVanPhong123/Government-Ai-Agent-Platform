'use client';
import { useUrlState } from '@/lib/hooks/useUrlState';
import { useDebounce } from '@/lib/hooks/useDebounce';
import { useAnomalies } from '@/lib/hooks/useAnomalies';
import { useCountries } from '@/lib/hooks/useCountries';
import { useDataState } from '@/lib/hooks/useDataState';
import AnomaliesTable from '@/components/tables/AnomaliesTable';
import Pagination from '@/components/ui/Pagination';
import { TableSkeleton } from '@/components/ui/Skeletons';

export default function AnomaliesPage() {
  const [country, setCountry] = useUrlState<string>('country', '');
  const [rawThreshold, setThreshold] = useUrlState<number>('threshold', 0.75);
  const [page, setPage] = useUrlState<number>('page', 1);
  
  const threshold = useDebounce(rawThreshold, 300);
  const { data: countries, isLoading: loadingCountries } = useCountries();
  
  const { data, isLoading, isEmpty, isError, error } = useDataState(
    useAnomalies({ country: country || undefined, threshold, limit: 200 })
  );

  const ITEMS_PER_PAGE = 15;
  const totalPages = Math.ceil((data?.length || 0) / ITEMS_PER_PAGE);
  const paginatedData = data?.slice((page - 1) * ITEMS_PER_PAGE, page * ITEMS_PER_PAGE) || [];

  const handleFilterChange = () => setPage(1);

  if (loadingCountries) return <TableSkeleton rows={1} />;
  if (isError) return <div className="p-4 bg-red-50 text-red-700 rounded">Lỗi: {error?.message}</div>;

  return (
    <div>
      <h1 className="text-2xl font-bold mb-4">Anomalies</h1>
      <div className="bg-white p-4 rounded shadow mb-6 flex flex-wrap gap-4 items-end">
        <div>
          <label className="block text-sm font-medium text-gray-700">Quốc gia</label>
          <select className="mt-1 block w-48 rounded border-gray-300 shadow-sm p-2"
            value={country} onChange={(e) => { setCountry(e.target.value); handleFilterChange(); }}>
            <option value="">Tất cả</option>
            {countries?.map((c) => (
              <option key={c.country_code} value={c.country_code}>{c.country_name}</option>
            ))}
          </select>
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700">Threshold ({rawThreshold.toFixed(2)})</label>
          <input type="range" min="0" max="1" step="0.01" value={rawThreshold}
            onChange={(e) => { setThreshold(parseFloat(e.target.value)); handleFilterChange(); }} className="w-48" />
        </div>
      </div>

      {isLoading ? <TableSkeleton rows={5} /> : isEmpty ? (
        <div className="text-center py-10 text-gray-500 bg-white rounded shadow">Không tìm thấy bất thường nào.</div>
      ) : (
        <>
          <AnomaliesTable data={paginatedData} />
          <Pagination currentPage={page} totalPages={totalPages} onPageChange={setPage} />
        </>
      )}
    </div>
  );
}