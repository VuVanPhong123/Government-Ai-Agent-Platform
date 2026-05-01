'use client';
import { useParams, useRouter } from 'next/navigation';
import { useCountryAnalytics } from '@/lib/hooks/useCountries';
import { ChartSkeleton, KpiCardSkeleton, PanelSkeleton } from '@/components/ui/Skeletons';
import CountryHeader from '@/components/country/CountryHeader';
import CountryKpiOverview from '@/components/country/CountryKpiOverview';
import Tabs from '@/components/ui/Tabs';
import ContextPanel from '@/components/ui/ContextPanel';
import { BarChart3, Activity, TrendingUp } from 'lucide-react';
import { useMemo } from 'react';

export default function CountryDetailPage() {
  const router = useRouter();
  const { code } = useParams();
  const normalizedCode = (code as string).toUpperCase();
  const { data, isLoading, isError, error } = useCountryAnalytics(normalizedCode);

  const latestYear = data && data.length > 0 ? data[data.length - 1].year : null;
  const countryName = useMemo(() => normalizedCode, [normalizedCode]);

  if (isLoading) {
    return (
      <div className="space-y-6">
        <div className="h-16 bg-white rounded-md animate-pulse" />
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <KpiCardSkeleton /><KpiCardSkeleton /><KpiCardSkeleton /><KpiCardSkeleton />
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          <div className="lg:col-span-8"><ChartSkeleton /></div>
          <div className="lg:col-span-4 space-y-4"><PanelSkeleton rows={3} /><PanelSkeleton rows={4} /></div>
        </div>
      </div>
    );
  }
  if (isError) return <div className="p-6 bg-red-50 text-red-700 rounded border border-red-200">Error: {error?.message}</div>;
  if (!data || data.length === 0) return <div className="p-12 text-center bg-white rounded-md border border-gray-200">Không có dữ liệu cho quốc gia này.</div>;

  const tabs = [
    { id: 'growth', label: 'Tăng trưởng', status: 'ok' as const, content: <div className="p-8 text-center text-gray-500 border border-dashed border-gray-300 rounded-md">Nội dung tab Tăng trưởng</div> },
    { id: 'fiscal', label: 'Tài khóa & Tiền tệ', status: 'ok' as const, content: <div className="p-8 text-center text-gray-500 border border-dashed border-gray-300 rounded-md">Nội dung tab Tài khóa</div> },
    { id: 'social', label: 'Xã hội', status: 'warning' as const, content: <div className="p-8 text-center text-gray-500 border border-dashed border-gray-300 rounded-md">Nội dung tab Xã hội</div> },
    { id: 'risk', label: 'Rủi ro', status: 'error' as const, content: <div className="p-8 text-center text-gray-500 border border-dashed border-gray-300 rounded-md">Nội dung tab Rủi ro</div> },
  ];

  const firstRow = data[0];
  const latestRow = data[data.length - 1];
  const contextItems = [
    { icon: BarChart3, label: 'Chu kỳ dữ liệu', value: `${firstRow.year} – ${latestRow.year}` },
    { icon: Activity, label: 'Tổng số quan sát', value: `${data.length} năm` },
    { icon: TrendingUp, label: 'Tăng trưởng mới nhất', value: latestRow.actual_growth != null ? `${latestRow.actual_growth.toFixed(2)}%` : 'N/A' },
  ];

  return (
    <div className="space-y-6">
      <CountryHeader countryCode={normalizedCode} countryName={countryName} latestYear={latestYear} onExport={() => console.log('Export')} onCompare={() => router.push(`/compare?countries=${normalizedCode}&indicator=rGDP_growth_YoY`)} />
      <CountryKpiOverview data={data} />
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        <div className="lg:col-span-8">
          <Tabs tabs={tabs} />
        </div>
        <div className="lg:col-span-4 space-y-4">
          <ContextPanel title="Tóm tắt nhanh" items={contextItems} />
        </div>
      </div>
    </div>
  );
}