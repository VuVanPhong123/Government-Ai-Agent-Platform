'use client';
import { useAnomalies } from '@/lib/hooks/useAnomalies';

interface AnomaliesTableProps {
  country?: string;
  threshold: number;
  limit?: number;
}

export default function AnomaliesTable({ country, threshold, limit = 100 }: AnomaliesTableProps) {
  const { data, isLoading, error } = useAnomalies({ country, threshold, limit });

  if (isLoading) return <div>Loading anomalies...</div>;
  if (error) return <div>Error: {error.message}</div>;
  if (!data || data.length === 0) return <div>No anomalies found.</div>;

  return (
    <div className="bg-white rounded shadow overflow-x-auto">
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Country</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Year</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Indicator</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actual Value</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Anomaly Score</th>
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {data.map((item: any, idx: number) => (
            <tr key={idx} className="hover:bg-gray-50">
              <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{item.country_name}</td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{item.year}</td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{item.indicator}</td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{item.actual_value?.toFixed(2)}</td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{item.anomaly_score?.toFixed(3)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}