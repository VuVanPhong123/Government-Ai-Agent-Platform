'use client';
import Link from 'next/link';
import { Bot, Globe2, Layers, ListChecks } from 'lucide-react';
import PageHeader from '@/components/ui/PageHeader';
import SectionCard from '@/components/ui/SectionCard';
import StatCard from '@/components/ui/StatCard';
import StateBlock from '@/components/ui/StateBlock';
import { useCountries } from '@/lib/hooks/useCountries';
import { useIndicators } from '@/lib/hooks/useIndicators';
import { useClusters } from '@/lib/hooks/useClusters';

function MetricErrorCard({ title, message }: { title: string; message: string }) {
  return (
    <div className="rounded-md border border-red-200 bg-red-50 px-4 py-4">
      <p className="text-sm font-semibold text-red-700">{title}</p>
      <p className="mt-1 text-xs text-red-600">{message}</p>
    </div>
  );
}

export default function DashboardPage() {
  const countriesQuery = useCountries();
  const indicatorsQuery = useIndicators();
  const clustersQuery = useClusters(2025);

  const clusterCount = clustersQuery.data ? new Set(clustersQuery.data.map((item) => item.cluster_id)).size : 0;
  const latestClusterYear =
    clustersQuery.data && clustersQuery.data.length > 0
      ? Math.max(...clustersQuery.data.map((item) => item.latest_valid_year ?? item.year))
      : 2025;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Tổng quan nền tảng dữ liệu kinh tế chính phủ"
        description="Nền tảng sử dụng dữ liệu kinh tế công khai, truy xuất BigQuery-direct và trợ lý dữ liệu AI để hỗ trợ phân tích quốc gia."
      />

      <section className="grid grid-cols-1 gap-4 lg:grid-cols-4">
        {countriesQuery.isError ? (
          <MetricErrorCard title="Tổng số quốc gia" message="Không tải được dữ liệu quốc gia." />
        ) : (
          <StatCard
            label="Tổng số quốc gia"
            value={countriesQuery.isLoading ? '...' : String(countriesQuery.data?.length ?? 0)}
            note="Từ API /api/v1/countries"
            icon={<Globe2 className="h-5 w-5" />}
          />
        )}
        {indicatorsQuery.isError ? (
          <MetricErrorCard title="Tổng số chỉ số" message="Không tải được danh mục chỉ số." />
        ) : (
          <StatCard
            label="Tổng số chỉ số"
            value={indicatorsQuery.isLoading ? '...' : String(indicatorsQuery.data?.length ?? 0)}
            note="Từ API /api/v1/indicators"
            icon={<ListChecks className="h-5 w-5" />}
          />
        )}
        {clustersQuery.isError ? (
          <MetricErrorCard title="Nhóm cấu trúc" message="Không tải được dữ liệu cụm năm 2025." />
        ) : (
          <StatCard
            label="Số cụm cấu trúc (2025)"
            value={clustersQuery.isLoading ? '...' : String(clusterCount)}
            note="Dùng /api/v1/analytics/clusters"
            icon={<Layers className="h-5 w-5" />}
          />
        )}
        <StatCard
          label="Trợ lý dữ liệu AI"
          value="Sẵn sàng"
          note="Kênh hỏi đáp dữ liệu kinh tế tự nhiên"
          icon={<Bot className="h-5 w-5" />}
          href="/chat"
        />
      </section>

      <SectionCard title="Luồng demo nhanh" description="Các bước truy cập nhanh cho kịch bản trình diễn chính">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
          <Link
            href="/countries/VNM"
            className="rounded-md border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-medium text-slate-800 hover:bg-slate-100"
          >
            Hồ sơ Việt Nam
          </Link>
          <Link
            href="/compare?countries=VNM,THA&indicator=govdebt_GDP&from=2010&to=2023"
            className="rounded-md border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-medium text-slate-800 hover:bg-slate-100"
          >
            So sánh Việt Nam và Thái Lan
          </Link>
          <Link
            href="/clusters?year=2025"
            className="rounded-md border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-medium text-slate-800 hover:bg-slate-100"
          >
            Nhóm cấu trúc năm 2025
          </Link>
          <Link
            href="/chat?q=So%20s%C3%A1nh%20n%E1%BB%A3%20c%C3%B4ng%20Vi%E1%BB%87t%20Nam%20v%C3%A0%20Th%C3%A1i%20Lan%20t%E1%BB%AB%202010%20%C4%91%E1%BA%BFn%202023"
            className="rounded-md border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-medium text-slate-800 hover:bg-slate-100"
          >
            Hỏi trợ lý AI
          </Link>
        </div>
      </SectionCard>

      <section className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <SectionCard title="Dữ liệu quốc gia và chỉ số">
          <p className="text-sm leading-6 text-slate-700">
            Danh sách quốc gia và chỉ số được đồng bộ từ nguồn dữ liệu công khai. Người dùng có thể tra cứu theo mã quốc gia,
            mở hồ sơ chi tiết, so sánh theo giai đoạn và theo dõi chất lượng dữ liệu theo năm.
          </p>
        </SectionCard>
        <SectionCard title="Nhóm cấu trúc và bất thường">
          <p className="text-sm leading-6 text-slate-700">
            Nhóm cấu trúc giúp so sánh các quốc gia có đặc điểm kinh tế tương đồng trong cùng năm dữ liệu. Màn hình bất thường
            hỗ trợ giám sát các điểm dữ liệu có mức lệch lớn theo ngưỡng.
          </p>
        </SectionCard>
      </section>

      {!countriesQuery.isLoading &&
      !indicatorsQuery.isLoading &&
      !clustersQuery.isLoading &&
      (countriesQuery.data?.length ?? 0) === 0 &&
      (indicatorsQuery.data?.length ?? 0) === 0 &&
      (clustersQuery.data?.length ?? 0) === 0 ? (
        <StateBlock
          mode="empty"
          title="Chưa có dữ liệu tổng quan"
          description="Hiện chưa tải được dữ liệu cho trang tổng quan. Vui lòng kiểm tra kết nối API backend."
        />
      ) : null}

      <p className="text-xs text-slate-500">Năm cụm dữ liệu gần nhất: {latestClusterYear}</p>
    </div>
  );
}
