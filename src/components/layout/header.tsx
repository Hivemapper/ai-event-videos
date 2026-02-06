"use client";

import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import { Video } from "lucide-react";
import { cn } from "@/lib/utils";

interface NavItem {
  href: string;
  label: string;
  isActive: (pathname: string, searchParams: URLSearchParams) => boolean;
}

const navItems: NavItem[] = [
  {
    href: "/",
    label: "Gallery",
    isActive: (pathname, searchParams) =>
      pathname === "/" && !searchParams.has("agent"),
  },
  {
    href: "/highlights",
    label: "Highlights",
    isActive: (pathname) => pathname.startsWith("/highlights"),
  },
  {
    href: "/?agent=true",
    label: "Agent",
    isActive: (pathname, searchParams) =>
      pathname === "/" && searchParams.has("agent"),
  },
];

interface HeaderProps {
  children?: React.ReactNode;
}

export function Header({ children }: HeaderProps) {
  const pathname = usePathname();
  const searchParams = useSearchParams();

  return (
    <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container mx-auto px-4 h-14 flex items-center justify-between">
        <div className="flex items-center gap-6">
          <Link href="/" className="flex items-center gap-2">
            <Video className="w-6 h-6 text-primary" />
            <span className="font-semibold text-lg">AI Event Videos</span>
          </Link>
          <nav className="flex items-center gap-1">
            {navItems.map((item) => {
              const active = item.isActive(pathname, searchParams);
              return (
                <Link
                  key={item.label}
                  href={item.href}
                  className={cn(
                    "px-3 py-1.5 rounded-md text-sm font-medium transition-colors",
                    active
                      ? "bg-primary/10 text-primary"
                      : "text-muted-foreground hover:text-foreground hover:bg-muted"
                  )}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
        </div>
        {children && (
          <div className="flex items-center gap-2">{children}</div>
        )}
      </div>
    </header>
  );
}
