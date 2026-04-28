'use client';
import { useFilterStore } from '@/lib/stores/filterStore';
import AnomaliesTable from '@/components/tables/AnomaliesTable';
import { useCountries } from '@/lib/hooks/useCountries';

export default function AnomaliesPage() {
  const { selectedCountry, anomalyThreshold, setSelectedCountry, setAnomalyThreshold } = useFilterStore();
  const { data: countries } = useCountries();

  return (
    <div>
      <h1 className="text-2xl font-bold mb-4">Anomalies</h1>
      <div className="bg-white p-4 rounded shadow mb-6 flex gap-4 items-end">
        <div>
          <label className="block text-sm font-medium text-gray-700">Country</label>
          <select
            className="mt-1 block w-48 rounded border-gray-300 shadow-sm"
            value={selectedCountry}
            onChange={(e) => setSelectedCountry(e.target.value)}
          >
            <option value="">All</option>
            {countries?.map((c: any) => (
              <option key={c.country_code} value={c.country_code}>{c.country_name}</option>
            ))}
          </select>
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700">Threshold (0-1)</label>
          <input
            type="range"
            min="0"
            max="1"
            step="0.01"
            value={anomalyThreshold}
            onChange={(e) => setAnomalyThreshold(parseFloat(e.target.value))}
            className="w-48"
          />
          <span className="ml-2 text-sm">{anomalyThreshold}</span>
        </div>
      </div>
      <AnomaliesTable country={selectedCountry || undefined} threshold={anomalyThreshold} />
    </div>
  );
}