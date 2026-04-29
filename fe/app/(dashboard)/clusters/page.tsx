'use client';
import { useUrlState } from '@/lib/hooks/useUrlState';
import { useClusters } from '@/lib/hooks/useClusters';
import { useDataState } from '@/lib/hooks/useDataState';
import ClusterPieChart from '@/components/charts/PieChart';
import { ChartSkeleton } from '@/components/ui/Skeletons';

export default function ClustersPage() {
  const [year, setYear] = useUrlState<number>('year', 2022);
  const { data: clusters, isLoading, isEmpty, isError, error } = useDataState(useClusters(year));

  if (isError) return <div className="p-4 bg-red-50 text-red-700 rounded">Lỗi: {error?.message}</div>;

  const clusterCounts: Record<number, number> = {};
  const grouped: Record<number, string[]> = {};
  
  if (!isEmpty && clusters) {
    clusters.forEach(item => {
      const id = item.cluster_id;
      clusterCounts[id] = (clusterCounts[id] || 0) + 1;
      if (!grouped[id]) grouped[id] = [];
      grouped[id].push(item.country_code);
    });
  }

  const pieData = Object.entries(clusterCounts).map(([id, count]) => ({
    cluster_id: parseInt(id),
    count,
  }));

  return (
    <div>
      <h1 className="text-2xl font-bold mb-4">Clusters</h1>
      <div className="mb-4 flex items-center gap-2">
        <label className="font-medium">Năm:</label>
        <select value={year} onChange={(e) => setYear(parseInt(e.target.value))} className="border rounded p-2">
          {[2000, 2010, 2020, 2022].map(y => <option key={y} value={y}>{y}</option>)}
        </select>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white p-4 rounded shadow min-h-[300px]">
          <h2 className="text-lg font-semibold mb-2">Phân bố</h2>
          {isLoading ? <ChartSkeleton /> : isEmpty ? (
            <div className="flex items-center justify-center h-48 text-gray-500">Chưa có dữ liệu cluster.</div>
          ) : <ClusterPieChart data={pieData} />}
        </div>
        <div className="bg-white p-4 rounded shadow">
          <h2 className="text-lg font-semibold mb-2">Quốc gia theo Cluster</h2>
          {Object.keys(grouped).sort().map(id => (
            <div key={id} className="mb-4">
              <h3 className="font-medium text-blue-600">Cluster {id}</h3>
              <div className="flex flex-wrap gap-2 mt-2">
                {grouped[parseInt(id)].map(code => (
                  <span key={code} className="bg-gray-100 px-3 py-1 rounded text-sm border">{code}</span>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}