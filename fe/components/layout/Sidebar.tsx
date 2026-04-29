"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useUIStore } from "@/lib/stores/uiStore";

const navItems = [
  { name: "Dashboard", path: "/" },
  { name: "Countries", path: "/countries" },
  { name: "Anomalies", path: "/anomalies" },
  { name: "Clusters", path: "/clusters" },
  { name: "Compare", path: "/compare" },
  { name: "AI Chat", path: "/chat" },
];

export default function Sidebar() {
  const pathname = usePathname();
  const { isSidebarOpen, setSidebarOpen } = useUIStore();

  return (
    <>
      {/* Mobile Overlay */}
      {isSidebarOpen && (
        <div className="fixed inset-0 bg-black/40 z-40 lg:hidden" onClick={() => setSidebarOpen(false)} />
      )}
      <aside className={`fixed inset-y-0 left-0 z-50 w-64 bg-gray-900 text-white transform transition-transform duration-300 lg:translate-x-0 ${isSidebarOpen ? 'translate-x-0' : '-translate-x-full'}`}>
        <div className="flex items-center justify-between p-4 border-b border-gray-700">
          <h1 className="text-xl font-bold">Gov AI Agent</h1>
          <button onClick={() => setSidebarOpen(false)} className="lg:hidden p-2 text-gray-300 hover:text-white">✕</button>
        </div>
        <nav className="p-4 space-y-1">
          {navItems.map((item) => (
            <Link
              key={item.path}
              href={item.path}
              onClick={() => setSidebarOpen(false)}
              className={`block py-2 px-3 rounded mb-1 ${pathname === item.path ? "bg-blue-600" : "hover:bg-gray-700"}`}
            >
              {item.name}
            </Link>
          ))}
        </nav>
      </aside>
    </>
  );
}