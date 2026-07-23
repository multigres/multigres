import { NextRequest, NextResponse } from "next/server";

export function middleware(request: NextRequest) {
  const apiUrl = process.env.MULTIADMIN_API_URL || "http://localhost:15000";
  const { pathname, search } = request.nextUrl;

  if (
    pathname.startsWith("/api/v1/") ||
    pathname.startsWith("/proxy/") ||
    pathname.startsWith("/multiadmin.")
  ) {
    return NextResponse.rewrite(new URL(`${apiUrl}${pathname}${search}`));
  }
  return NextResponse.next();
}

export const config = {
  // The connect-web client posts to "/multiadmin.<Service>/<Method>"; the
  // ":path*" form doesn't match because the segment after "multiadmin." has no
  // leading slash, so use a regex matching the whole "/multiadmin.*" prefix.
  matcher: ["/api/v1/:path*", "/proxy/:path*", "/:path(multiadmin\\..*)"],
};
