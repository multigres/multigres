import { NextRequest, NextResponse } from "next/server";

export function middleware(request: NextRequest) {
  const apiUrl = process.env.MULTIADMIN_API_URL || "http://localhost:15000";
  const { pathname, search } = request.nextUrl;

  if (pathname.startsWith("/api/v1/") || pathname.startsWith("/proxy/")) {
    return NextResponse.rewrite(new URL(`${apiUrl}${pathname}${search}`));
  }
  return NextResponse.next();
}

export const config = {
  matcher: ["/api/v1/:path*", "/proxy/:path*"],
};
