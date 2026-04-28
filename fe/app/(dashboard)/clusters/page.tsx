'use client';
import { useState } from 'react';
import { useClusters } from '@/lib/hooks/useClusters';
import ClusterPieChart from '@/components/charts/PieChart';

export default function ClustersPage() {
  const [year, setYear] = useState(2022);
  const { data: clusters, isLoading } = useClusters(year);

  if (isLoading) return <div>Loading clusters...</div>;

  const clusterCounts = clusters?.reduce((acc: Record<number, number>, item: any) => {
    const id = item.cluster_id;
    acc[id] = (acc[id] || 0) + 1;
    return acc;
  }, {} as Record<number, number>);
  
  const pieData: { cluster_id: number; count: number }[] = Object.entries(clusterCounts || {}).map(([cluster_id, count]) => ({
    cluster_id: parseInt(cluster_id),
    count: Number(count),
  }));

  const grouped = clusters?.reduce((acc: Record<number, string[]>, item: any) => {
    const id = item.cluster_id;
    if (!acc[id]) acc[id] = [];
    acc[id].push(item.country_code);
    return acc;
  }, {} as Record<number, string[]>);

  return (
    <div>
      <h1 className="text-2xl font-bold mb-4">Clusters</h1>
      <div className="mb-4">
        <label className="mr-2">Year:</label>
        <select value={year} onChange={(e) => setYear(parseInt(e.target.value))} className="border rounded p-1">
          <option value={2000}>2000</option>
          <option value={2010}>2010</option>
          <option value={2020}>2020</option>
          <option value={2022}>2022</option>
        </select>
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white p-4 rounded shadow">
          <h2 className="text-lg font-semibold mb-2">Distribution</h2>
          <ClusterPieChart data={pieData} />
        </div>
        <div className="bg-white p-4 rounded shadow">
          <h2 className="text-lg font-semibold mb-2">Countries by Cluster</h2>
          {Object.keys(grouped || {})
            .sort()
            .map((cluster) => (
              <div key={cluster} className="mb-4">
                <h3 className="font-medium text-blue-600">Cluster {cluster}</h3>
                <div className="flex flex-wrap gap-1 mt-1">
                  {grouped![parseInt(cluster)].map((code: string) => (
                    <span key={code} className="bg-gray-200 px-2 py-1 rounded text-sm">
                      {code}
                    </span>
                  ))}
                </div>
              </div>
            ))}
        </div>
      </div>
    </div>
  );
}