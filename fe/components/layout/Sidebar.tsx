"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

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
  return (
    <aside className="w-64 bg-gray-900 text-white h-screen p-4 fixed left-0 top-0 overflow-y-auto">
      <h1 className="text-xl font-bold mb-6">Gov AI Agent</h1>
      <nav>
        {navItems.map((item) => (
          <Link
            key={item.path}
            href={item.path}
            className={`block py-2 px-3 rounded mb-1 ${
              pathname === item.path ? "bg-blue-600" : "hover:bg-gray-700"
            }`}
          >
            {item.name}
          </Link>
        ))}
      </nav>
    </aside>
  );
}