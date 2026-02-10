"use client";

import { useState, useEffect, Suspense } from "react";
import { useTheme } from "next-themes";
import {
  Settings,
  Key,
  Map,
  Check,
  AlertCircle,
  Moon,
  Sun,
  Camera,
  RefreshCw,
  Sparkles,
  FileText,
  Save,
  Gauge,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Header } from "@/components/layout/header";
import {
  getApiKey,
  setApiKey,
  clearApiKey,
  getMapboxToken,
  setMapboxToken,
  clearMapboxToken,
  getAnthropicKey,
  setAnthropicKey,
  clearAnthropicKey,
  getCameraIntrinsics,
  setCameraIntrinsics,
  fetchCameraIntrinsics,
  calculateFOV,
  DevicesResponse,
  getSpeedUnit,
  setSpeedUnit,
  SpeedUnit,
} from "@/lib/api";
import { cn } from "@/lib/utils";

function SettingsContent() {
  const { theme, setTheme } = useTheme();

  // Beemaps API Key state
  const [apiKeyInput, setApiKeyInput] = useState("");
  const [hasApiKey, setHasApiKey] = useState(false);
  const [apiKeySaved, setApiKeySaved] = useState(false);

  // Mapbox Token state
  const [mapboxInput, setMapboxInput] = useState("");
  const [hasMapboxToken, setHasMapboxToken] = useState(false);
  const [mapboxSaved, setMapboxSaved] = useState(false);

  // Anthropic API Key state
  const [anthropicInput, setAnthropicInput] = useState("");
  const [hasAnthropicKey, setHasAnthropicKey] = useState(false);
  const [anthropicSaved, setAnthropicSaved] = useState(false);

  // Camera Intrinsics state
  const [cameraIntrinsics, setCameraIntrinsicsState] = useState<DevicesResponse | null>(null);
  const [isLoadingIntrinsics, setIsLoadingIntrinsics] = useState(false);
  const [intrinsicsError, setIntrinsicsError] = useState<string | null>(null);

  // Speed unit state
  const [speedUnitValue, setSpeedUnitValue] = useState<SpeedUnit>("mph");

  // CLAUDE.md state
  const [claudeMdContent, setClaudeMdContent] = useState("");
  const [claudeMdLoaded, setClaudeMdLoaded] = useState(false);
  const [claudeMdSaving, setClaudeMdSaving] = useState(false);
  const [claudeMdSaved, setClaudeMdSaved] = useState(false);
  const [claudeMdError, setClaudeMdError] = useState<string | null>(null);

  useEffect(() => {
    // Load Beemaps API key
    const key = getApiKey();
    setHasApiKey(!!key);
    setApiKeyInput(key ? "••••••••" + key.slice(-4) : "");

    // Load Mapbox token
    const token = getMapboxToken();
    setHasMapboxToken(!!token);
    setMapboxInput(token ? "••••••••" + token.slice(-4) : "");

    // Load Anthropic API key
    const anthropic = getAnthropicKey();
    setHasAnthropicKey(!!anthropic);
    setAnthropicInput(anthropic ? "••••••••" + anthropic.slice(-4) : "");

    // Load camera intrinsics
    setCameraIntrinsicsState(getCameraIntrinsics());

    // Load speed unit
    setSpeedUnitValue(getSpeedUnit());
  }, []);

  // API key handlers
  const handleSaveApiKey = () => {
    if (apiKeyInput && !apiKeyInput.startsWith("••••")) {
      setApiKey(apiKeyInput);
      setHasApiKey(true);
      setApiKeySaved(true);
      setTimeout(() => setApiKeySaved(false), 2000);
    }
  };

  const handleClearApiKey = () => {
    clearApiKey();
    setApiKeyInput("");
    setHasApiKey(false);
  };

  const handleApiKeyInputChange = (value: string) => {
    if (apiKeyInput.startsWith("••••") && value.length > apiKeyInput.length) {
      setApiKeyInput(value.slice(apiKeyInput.length));
    } else {
      setApiKeyInput(value);
    }
  };

  // Mapbox handlers
  const handleSaveMapbox = () => {
    if (mapboxInput && !mapboxInput.startsWith("••••")) {
      setMapboxToken(mapboxInput);
      setHasMapboxToken(true);
      setMapboxSaved(true);
      setTimeout(() => setMapboxSaved(false), 2000);
    }
  };

  const handleClearMapbox = () => {
    clearMapboxToken();
    setMapboxInput("");
    setHasMapboxToken(false);
  };

  const handleMapboxInputChange = (value: string) => {
    if (mapboxInput.startsWith("••••") && value.length > mapboxInput.length) {
      setMapboxInput(value.slice(mapboxInput.length));
    } else {
      setMapboxInput(value);
    }
  };

  // Anthropic handlers
  const handleSaveAnthropic = () => {
    if (anthropicInput && !anthropicInput.startsWith("••••")) {
      setAnthropicKey(anthropicInput);
      setHasAnthropicKey(true);
      setAnthropicSaved(true);
      setTimeout(() => setAnthropicSaved(false), 2000);
    }
  };

  const handleClearAnthropic = () => {
    clearAnthropicKey();
    setAnthropicInput("");
    setHasAnthropicKey(false);
  };

  const handleAnthropicInputChange = (value: string) => {
    if (anthropicInput.startsWith("••••") && value.length > anthropicInput.length) {
      setAnthropicInput(value.slice(anthropicInput.length));
    } else {
      setAnthropicInput(value);
    }
  };

  // Camera intrinsics handler
  const handleFetchIntrinsics = async () => {
    setIsLoadingIntrinsics(true);
    setIntrinsicsError(null);
    try {
      const data = await fetchCameraIntrinsics();
      setCameraIntrinsics(data);
      setCameraIntrinsicsState(data);
    } catch (err) {
      setIntrinsicsError(err instanceof Error ? err.message : "Failed to fetch");
    } finally {
      setIsLoadingIntrinsics(false);
    }
  };

  // CLAUDE.md handlers
  const handleLoadClaudeMd = async () => {
    try {
      const res = await fetch("/api/claude-md");
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setClaudeMdContent(data.content);
      setClaudeMdLoaded(true);
      setClaudeMdError(null);
    } catch (err) {
      setClaudeMdError(err instanceof Error ? err.message : "Failed to load");
    }
  };

  const handleSaveClaudeMd = async () => {
    setClaudeMdSaving(true);
    setClaudeMdError(null);
    try {
      const res = await fetch("/api/claude-md", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ content: claudeMdContent }),
      });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setClaudeMdSaved(true);
      setTimeout(() => setClaudeMdSaved(false), 2000);
    } catch (err) {
      setClaudeMdError(err instanceof Error ? err.message : "Failed to save");
    } finally {
      setClaudeMdSaving(false);
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <Header />

      <main className="container mx-auto px-4 py-8 max-w-2xl">
        <div className="space-y-6">
          <div>
            <h1 className="text-2xl font-bold">Settings</h1>
            <p className="text-muted-foreground">
              Configure API keys, appearance, and camera settings
            </p>
          </div>

          <Tabs defaultValue="general" className="w-full">
            <TabsList className="w-full">
              <TabsTrigger value="general" className="flex-1">
                <Settings className="w-4 h-4 mr-2" />
                General
              </TabsTrigger>
              <TabsTrigger value="camera" className="flex-1">
                <Camera className="w-4 h-4 mr-2" />
                Camera Specs
              </TabsTrigger>
              <TabsTrigger value="claude-md" className="flex-1" onClick={() => {
                if (!claudeMdLoaded) handleLoadClaudeMd();
              }}>
                <FileText className="w-4 h-4 mr-2" />
                CLAUDE.md
              </TabsTrigger>
            </TabsList>

            <TabsContent value="general" className="space-y-6 py-4">
              {/* Appearance */}
              <div className="space-y-2">
                <label className="text-sm font-medium flex items-center gap-2">
                  {theme === "dark" ? <Moon className="w-4 h-4" /> : <Sun className="w-4 h-4" />}
                  Appearance
                </label>
                <div className="flex gap-2">
                  <Button
                    variant={theme === "light" ? "default" : "outline"}
                    size="sm"
                    onClick={() => setTheme("light")}
                    className="flex-1"
                  >
                    <Sun className="w-4 h-4 mr-2" />
                    Light
                  </Button>
                  <Button
                    variant={theme === "dark" ? "default" : "outline"}
                    size="sm"
                    onClick={() => setTheme("dark")}
                    className="flex-1"
                  >
                    <Moon className="w-4 h-4 mr-2" />
                    Dark
                  </Button>
                </div>
              </div>

              {/* Speed Unit */}
              <div className="space-y-2">
                <label className="text-sm font-medium flex items-center gap-2">
                  <Gauge className="w-4 h-4" />
                  Speed Unit
                </label>
                <div className="flex gap-2">
                  <Button
                    variant={speedUnitValue === "mph" ? "default" : "outline"}
                    size="sm"
                    onClick={() => { setSpeedUnit("mph"); setSpeedUnitValue("mph"); }}
                    className="flex-1"
                  >
                    mph
                  </Button>
                  <Button
                    variant={speedUnitValue === "kmh" ? "default" : "outline"}
                    size="sm"
                    onClick={() => { setSpeedUnit("kmh"); setSpeedUnitValue("kmh"); }}
                    className="flex-1"
                  >
                    km/h
                  </Button>
                </div>
              </div>

              {/* Beemaps API Key */}
              <div className="space-y-2">
                <label className="text-sm font-medium flex items-center gap-2">
                  <Key className="w-4 h-4" />
                  Beemaps API Key
                </label>
                <div className="flex gap-2">
                  <Input
                    type="password"
                    placeholder="Enter your Beemaps API key"
                    value={apiKeyInput}
                    onChange={(e) => handleApiKeyInputChange(e.target.value)}
                    className="flex-1"
                  />
                  <Button
                    onClick={handleSaveApiKey}
                    disabled={!apiKeyInput || apiKeyInput.startsWith("••••")}
                  >
                    {apiKeySaved ? (
                      <>
                        <Check className="w-4 h-4 mr-1" />
                        Saved
                      </>
                    ) : (
                      "Save"
                    )}
                  </Button>
                </div>
                {hasApiKey && (
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-green-600 flex items-center gap-1">
                      <Check className="w-3 h-3" />
                      API key configured
                    </span>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={handleClearApiKey}
                      className="text-destructive hover:text-destructive"
                    >
                      Clear
                    </Button>
                  </div>
                )}
              </div>

              {/* Mapbox Token */}
              <div className="space-y-2">
                <label className="text-sm font-medium flex items-center gap-2">
                  <Map className="w-4 h-4" />
                  Mapbox Access Token
                </label>
                <div className="flex gap-2">
                  <Input
                    type="password"
                    placeholder="Enter your Mapbox token"
                    value={mapboxInput}
                    onChange={(e) => handleMapboxInputChange(e.target.value)}
                    className="flex-1"
                  />
                  <Button
                    onClick={handleSaveMapbox}
                    disabled={!mapboxInput || mapboxInput.startsWith("••••")}
                  >
                    {mapboxSaved ? (
                      <>
                        <Check className="w-4 h-4 mr-1" />
                        Saved
                      </>
                    ) : (
                      "Save"
                    )}
                  </Button>
                </div>
                {hasMapboxToken && (
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-green-600 flex items-center gap-1">
                      <Check className="w-3 h-3" />
                      Mapbox token configured
                    </span>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={handleClearMapbox}
                      className="text-destructive hover:text-destructive"
                    >
                      Clear
                    </Button>
                  </div>
                )}
              </div>

              {/* Anthropic API Key */}
              <div className="space-y-2">
                <label className="text-sm font-medium flex items-center gap-2">
                  <Sparkles className="w-4 h-4" />
                  Anthropic API Key
                </label>
                <div className="flex gap-2">
                  <Input
                    type="password"
                    placeholder="Enter your Anthropic API key"
                    value={anthropicInput}
                    onChange={(e) => handleAnthropicInputChange(e.target.value)}
                    className="flex-1"
                  />
                  <Button
                    onClick={handleSaveAnthropic}
                    disabled={!anthropicInput || anthropicInput.startsWith("••••")}
                  >
                    {anthropicSaved ? (
                      <>
                        <Check className="w-4 h-4 mr-1" />
                        Saved
                      </>
                    ) : (
                      "Save"
                    )}
                  </Button>
                </div>
                {hasAnthropicKey && (
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-green-600 flex items-center gap-1">
                      <Check className="w-3 h-3" />
                      Anthropic key configured
                    </span>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={handleClearAnthropic}
                      className="text-destructive hover:text-destructive"
                    >
                      Clear
                    </Button>
                  </div>
                )}
                <p className="text-xs text-muted-foreground">
                  Required for the AI Filter Agent. Uses Claude to interpret natural language queries.
                </p>
              </div>

              <div
                className={cn(
                  "p-3 rounded-lg text-sm",
                  "bg-muted text-muted-foreground"
                )}
              >
                <div className="flex items-start gap-2">
                  <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
                  <div className="space-y-2">
                    <p>Your keys are stored locally in your browser.</p>
                    <p>
                      Get your Beemaps API key from the{" "}
                      <a
                        href="https://beemaps.com/developer"
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-primary hover:underline"
                      >
                        Beemaps Developer Portal
                      </a>
                      .
                    </p>
                    <p>
                      Get your Mapbox token from{" "}
                      <a
                        href="https://account.mapbox.com/access-tokens/"
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-primary hover:underline"
                      >
                        Mapbox Account
                      </a>
                      .
                    </p>
                    <p>
                      Get your Anthropic API key from the{" "}
                      <a
                        href="https://console.anthropic.com/settings/keys"
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-primary hover:underline"
                      >
                        Anthropic Console
                      </a>
                      .
                    </p>
                  </div>
                </div>
              </div>
            </TabsContent>

            <TabsContent value="camera" className="space-y-6 py-4">
              {/* Fetch Button */}
              <div className="space-y-2">
                <label className="text-sm font-medium flex items-center gap-2">
                  <Camera className="w-4 h-4" />
                  Hivemapper Device Intrinsics
                </label>
                <Button
                  variant="outline"
                  onClick={handleFetchIntrinsics}
                  disabled={isLoadingIntrinsics}
                  className="w-full"
                >
                  {isLoadingIntrinsics ? (
                    <>
                      <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                      Fetching...
                    </>
                  ) : (
                    <>
                      <RefreshCw className="w-4 h-4 mr-2" />
                      {cameraIntrinsics ? "Refresh from API" : "Fetch from API"}
                    </>
                  )}
                </Button>
                {intrinsicsError && (
                  <p className="text-sm text-red-600">{intrinsicsError}</p>
                )}
              </div>

              {cameraIntrinsics?.bee && (
                <div className="space-y-2">
                  <h4 className="text-sm font-medium">Bee Camera</h4>
                  <div className="p-3 rounded-lg bg-muted text-sm">
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <span className="text-muted-foreground text-xs">Horizontal FOV</span>
                        <p className="font-mono font-medium">
                          {calculateFOV(cameraIntrinsics.bee.focal).toFixed(1)}°
                        </p>
                      </div>
                      <div>
                        <span className="text-muted-foreground text-xs">Focal Length</span>
                        <p className="font-mono font-medium">
                          {cameraIntrinsics.bee.focal.toFixed(4)}
                        </p>
                      </div>
                      <div>
                        <span className="text-muted-foreground text-xs">k1 (radial)</span>
                        <p className="font-mono font-medium">
                          {cameraIntrinsics.bee.k1.toFixed(4)}
                        </p>
                      </div>
                      <div>
                        <span className="text-muted-foreground text-xs">k2 (radial)</span>
                        <p className="font-mono font-medium">
                          {cameraIntrinsics.bee.k2.toFixed(4)}
                        </p>
                      </div>
                      {cameraIntrinsics.bee.k3 && (
                        <div>
                          <span className="text-muted-foreground text-xs">k3 (radial)</span>
                          <p className="font-mono font-medium">
                            {cameraIntrinsics.bee.k3.toFixed(4)}
                          </p>
                        </div>
                      )}
                      {cameraIntrinsics.bee.p1 && (
                        <div>
                          <span className="text-muted-foreground text-xs">p1 (tangential)</span>
                          <p className="font-mono font-medium">
                            {cameraIntrinsics.bee.p1.toFixed(6)}
                          </p>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              )}

              {!cameraIntrinsics?.bee && (
                <div className="p-4 rounded-lg bg-muted text-sm text-muted-foreground text-center">
                  Click &quot;Fetch from API&quot; to load camera intrinsics from the Beemaps API.
                </div>
              )}

              <div className={cn("p-3 rounded-lg text-sm", "bg-muted text-muted-foreground")}>
                <div className="flex items-start gap-2">
                  <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
                  <div className="space-y-1">
                    <p>Camera intrinsics notes:</p>
                    <ul className="list-disc list-inside text-xs space-y-1">
                      <li>Calculating field of view</li>
                      <li>Undistorting images for CV tasks</li>
                      <li>Estimating feature visibility in frames</li>
                      <li>All AI Event Videos are from the Bee camera</li>
                    </ul>
                  </div>
                </div>
              </div>
            </TabsContent>

            <TabsContent value="claude-md" className="space-y-4 py-4">
              <div className="space-y-2">
                <label className="text-sm font-medium flex items-center gap-2">
                  <FileText className="w-4 h-4" />
                  Project Instructions (CLAUDE.md)
                </label>
                <p className="text-xs text-muted-foreground">
                  This file provides instructions to Claude Code for working with this project.
                </p>
              </div>

              {claudeMdError && (
                <div className="flex items-center gap-2 p-3 bg-destructive/10 text-destructive rounded-lg text-sm">
                  <AlertCircle className="w-4 h-4 shrink-0" />
                  {claudeMdError}
                </div>
              )}

              <textarea
                value={claudeMdContent}
                onChange={(e) => setClaudeMdContent(e.target.value)}
                className="w-full h-[500px] p-4 rounded-lg border bg-muted font-mono text-sm resize-y focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder={claudeMdLoaded ? "(empty file)" : "Switch to this tab to load CLAUDE.md..."}
                readOnly={!claudeMdLoaded}
              />

              <div className="flex items-center gap-2">
                <Button onClick={handleSaveClaudeMd} disabled={claudeMdSaving || !claudeMdLoaded}>
                  {claudeMdSaving ? (
                    <>
                      <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                      Saving...
                    </>
                  ) : claudeMdSaved ? (
                    <>
                      <Check className="w-4 h-4 mr-2" />
                      Saved
                    </>
                  ) : (
                    <>
                      <Save className="w-4 h-4 mr-2" />
                      Save
                    </>
                  )}
                </Button>
                <Button variant="outline" onClick={handleLoadClaudeMd}>
                  <RefreshCw className="w-4 h-4 mr-2" />
                  Reload
                </Button>
              </div>
            </TabsContent>
          </Tabs>
        </div>
      </main>
    </div>
  );
}

export default function SettingsPage() {
  return (
    <Suspense fallback={<div className="min-h-screen bg-background" />}>
      <SettingsContent />
    </Suspense>
  );
}
