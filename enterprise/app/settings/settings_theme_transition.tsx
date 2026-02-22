/**
 * Canvas which renders underneath the settings page content and provides a cool
 * animation when switching between light and dark mode.
 *
 * Treat this like a "temporary art installation." If it ever becomes a chore to
 * maintain, anyone should feel free to delete it!
 */

import React from "react";
import { AnimationLoop } from "../../../app/util/animation_loop";
import { interpolateColor } from "../../../app/util/color";
import { clamp, lerp, randomBetween } from "../../../app/util/math";

const TRANSITION_DURATION_MS = 500;
const COLOR_EASE_WINDOW = 0.75;
const BACKGROUND_FADE_POWER = 1.5;
const SHAPE_FADE_POWER = 1.5;
const SHAPE_FADE_IN_DURATION_FRACTION = 0.25;

const INK_LAYER_START_COLORS = ["#1A237E", "#4A148C", "#0D47A1", "#000000"] as const;
const INK_SPAWN_WINDOW_PROGRESS = 0.25;
const INK_INITIAL_RADIUS_MIN_PX = 5;
const INK_INITIAL_RADIUS_MAX_PX = 10;
const MAX_INK_DROPS = 32;

const LIGHT_RAY_START_COLORS = ["#F9FBE7", "#FFF9C4", "#FFECB3", "#FFFFFF"] as const;
const LIGHT_RAY_ANGLE_RADIANS = (3 * Math.PI) / 4;
const LIGHT_RAY_THICKNESS_GROWTH_POWER = 2.5;
const LIGHT_RAY_LENGTH_OVERSCAN_MULTIPLIER = 2.0;
const RAY_SPAWN_WINDOW_PROGRESS = 0.25;
const RAY_INITIAL_THICKNESS_MIN_PX = 1;
const RAY_INITIAL_THICKNESS_MAX_PX = 2;
const MAX_LIGHT_RAYS = 64;

interface SettingsThemeTransitionProps {
  dark: boolean;
}

export default function SettingsThemeTransition({ dark }: SettingsThemeTransitionProps) {
  const canvasRef = React.useRef<HTMLCanvasElement>(null);
  const animationRef = React.useRef<TransitionAnimation | null>(null);

  React.useEffect(() => {
    const canvas = canvasRef.current;
    const hostElement = canvas?.parentElement;
    if (!canvas || !hostElement) return;

    const animation = new TransitionAnimation(canvas);
    animationRef.current = animation;
    const reducedMotionQuery =
      typeof window.matchMedia === "function" ? window.matchMedia("(prefers-reduced-motion: reduce)") : null;
    const syncReducedMotionPreference = () => {
      animation.setPrefersReducedMotion(Boolean(reducedMotionQuery?.matches));
    };
    syncReducedMotionPreference();
    animation.resizeToParent();
    animation.redraw();
    const onResize = () => {
      animation.resizeToParent();
      animation.redraw();
    };
    const onReducedMotionChange = () => syncReducedMotionPreference();
    const resizeObserver = typeof ResizeObserver !== "undefined" ? new ResizeObserver(onResize) : undefined;
    resizeObserver?.observe(hostElement);
    window.addEventListener("resize", onResize);
    if (reducedMotionQuery) {
      if (typeof reducedMotionQuery.addEventListener === "function") {
        reducedMotionQuery.addEventListener("change", onReducedMotionChange);
      } else {
        reducedMotionQuery.addListener(onReducedMotionChange);
      }
    }

    return () => {
      window.removeEventListener("resize", onResize);
      if (reducedMotionQuery) {
        if (typeof reducedMotionQuery.removeEventListener === "function") {
          reducedMotionQuery.removeEventListener("change", onReducedMotionChange);
        } else {
          reducedMotionQuery.removeListener(onReducedMotionChange);
        }
      }
      resizeObserver?.disconnect();
      animation.destroy();
      animationRef.current = null;
    };
  }, []);

  React.useEffect(() => {
    if (!animationRef.current) return;
    animationRef.current.setTheme(dark);
  }, [dark]);

  return <canvas ref={canvasRef} className="settings-underlay" aria-hidden />;
}

function getThemeBackground(element: HTMLElement) {
  let current: HTMLElement | null = element;
  while (current) {
    const resolvedPrimary = resolveCssVariableColor(current, "--color-bg-primary");
    if (resolvedPrimary) {
      return resolvedPrimary;
    }

    const computed = window.getComputedStyle(current);
    if (!isTransparentColor(computed.backgroundColor)) {
      return computed.backgroundColor;
    }
    current = current.parentElement;
  }

  const rootVariableColor = resolveCssVariableColor(document.documentElement, "--color-bg-primary");
  if (rootVariableColor) {
    return rootVariableColor;
  }

  const bodyBackground = window.getComputedStyle(document.body).backgroundColor;
  if (!isTransparentColor(bodyBackground)) {
    return bodyBackground;
  }

  return "rgb(255, 255, 255)";
}

function resolveCssVariableColor(hostElement: HTMLElement, variableName: string): string | null {
  const probe = document.createElement("span");
  probe.style.position = "absolute";
  probe.style.left = "0";
  probe.style.top = "0";
  probe.style.width = "1px";
  probe.style.height = "1px";
  probe.style.pointerEvents = "none";
  probe.style.opacity = "0";
  probe.style.backgroundColor = `var(${variableName})`;
  hostElement.appendChild(probe);
  const color = window.getComputedStyle(probe).backgroundColor;
  hostElement.removeChild(probe);

  return isTransparentColor(color) ? null : color;
}

function isTransparentColor(color: string): boolean {
  const normalized = color.toLowerCase().replace(/\s+/g, "");
  return (
    normalized === "" ||
    normalized === "transparent" ||
    normalized === "rgba(0,0,0,0)" ||
    normalized === "rgb(0,0,0,0)" ||
    normalized === "rgb(0,0,0/0)" ||
    normalized === "rgb(0,0,0/0%)" ||
    normalized === "rgb(000/0)" ||
    normalized === "rgb(000/0%)"
  );
}

type InkSatellite = {
  angleRadians: number;
  distanceFactor: number;
  radiusFactor: number;
  driftFactor: number;
};

type InkDrop = {
  x: number;
  y: number;
  layerIndex: number;
  startTimeMs: number;
  growthDurationMs: number;
  initialRadius: number;
  finalRadius: number;
  satellites: InkSatellite[];
};

type LightRay = {
  startX: number;
  startY: number;
  startColor: string;
  angleRadians: number;
  startTimeMs: number;
  growthDurationMs: number;
  initialThickness: number;
  finalThickness: number;
  finalLength: number;
};

type InkCircle = {
  x: number;
  y: number;
  radius: number;
};

type Point = {
  x: number;
  y: number;
};

class TransitionAnimation {
  private loop = new AnimationLoop((dt: number) => this.step(dt));
  private canvas: HTMLCanvasElement;
  private ctx: CanvasRenderingContext2D;
  private shapeLayerCanvas?: HTMLCanvasElement;

  // timeElapsed since the start of the transition, in milliseconds.
  private timeElapsed = 0;

  // Whether we are at or transitioning to a dark BG.
  // False if we are at or transitioning to a light BG.
  private prevDark: boolean | null = null;
  private targetDark: boolean | null = null;
  // targetBG is the bg color we're at or transitioning to.
  // It should be a dark color if and only if targetDark is true.
  private prevBGColor: string | null = null;
  private targetBGColor: string | null = null;
  private prefersReducedMotion = false;

  private inkDrops: InkDrop[] = [];
  private lightRays: LightRay[] = [];

  constructor(canvas: HTMLCanvasElement) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d")!;
  }

  resizeToParent() {
    const rect = this.canvas.parentElement!.getBoundingClientRect();
    const width = Math.max(1, Math.ceil(rect.width));
    const height = Math.max(1, Math.ceil(rect.height));
    const dpr = window.devicePixelRatio || 1;
    const scaledWidth = Math.max(1, Math.round(width * dpr));
    const scaledHeight = Math.max(1, Math.round(height * dpr));

    if (this.canvas.width === scaledWidth && this.canvas.height === scaledHeight) {
      return;
    }

    this.canvas.width = scaledWidth;
    this.canvas.height = scaledHeight;
    this.canvas.style.width = `${width}px`;
    this.canvas.style.height = `${height}px`;
    this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  /** Force redraw (to be called when the canvas is resized). */
  redraw() {
    this.paint();
  }

  setPrefersReducedMotion(prefersReducedMotion: boolean) {
    if (this.prefersReducedMotion === prefersReducedMotion) return;
    this.prefersReducedMotion = prefersReducedMotion;
    if (prefersReducedMotion) {
      this.loop.stop();
      this.clearScene();
      this.timeElapsed = 0;
    }
    this.paint();
  }

  setTheme(dark: boolean) {
    if (this.targetDark === null) {
      // Initial update. Just set prev == target and paint.
      this.prevDark = dark;
      this.targetDark = dark;
      this.prevBGColor = getThemeBackground(this.canvas.parentElement!);
      this.targetBGColor = this.prevBGColor;
      this.paint();
      return;
    }

    // Update prev / target.
    this.prevDark = this.targetDark;
    this.prevBGColor = this.targetBGColor;
    this.targetDark = dark;
    this.targetBGColor = getThemeBackground(this.canvas.parentElement!);

    if (this.prevDark !== this.targetDark) {
      if (this.prefersReducedMotion) {
        // Respect reduced motion by switching instantly without shape animation.
        this.loop.stop();
        this.clearScene();
        this.timeElapsed = 0;
        this.paint();
        return;
      }
      // Theme changed; begin new transition.
      this.loop.stop();
      this.clearScene();
      this.timeElapsed = 0;
      this.spawnInitialShape();
      this.loop.start();
      this.paint();
      return;
    }

    this.paint();
  }

  private step(dt: number) {
    this.timeElapsed += dt;
    this.updateScene();

    this.paint();
    if (this.timeElapsed >= TRANSITION_DURATION_MS) {
      this.loop.stop();
      this.clearScene();
      this.paint();
    }
  }

  private paint() {
    const canvasWidth = Math.max(1, this.canvas.clientWidth);
    const canvasHeight = Math.max(1, this.canvas.clientHeight);
    if (!this.loop.isRunning()) {
      // We're not transitioning.
      const color = this.targetBGColor ?? this.prevBGColor ?? getThemeBackground(this.canvas.parentElement!);
      this.ctx.fillStyle = color;
      this.ctx.fillRect(0, 0, canvasWidth, canvasHeight);
      return;
    }

    const from = this.prevBGColor ?? this.targetBGColor ?? getThemeBackground(this.canvas.parentElement!);
    const to = this.targetBGColor ?? from;
    const progress = Math.max(0, Math.min(1, this.timeElapsed / TRANSITION_DURATION_MS));
    // Repaint a stable base each frame with eased BG interpolation.
    const backgroundProgress = 1 - Math.pow(1 - progress, BACKGROUND_FADE_POWER);
    this.ctx.fillStyle = interpolateColor(from, to, backgroundProgress);
    this.ctx.fillRect(0, 0, canvasWidth, canvasHeight);

    const targetBackground = this.targetBGColor ?? to;

    const layerContext = this.getShapeLayerContext(canvasWidth, canvasHeight);
    if (!layerContext) {
      return;
    }
    if (this.targetDark) {
      this.drawInkDrops(layerContext, targetBackground);
    } else {
      this.drawLightRays(layerContext, targetBackground);
    }
    const shapeFadeProgress =
      SHAPE_FADE_IN_DURATION_FRACTION <= 0 ? 1 : clamp(progress / SHAPE_FADE_IN_DURATION_FRACTION, 0, 1);
    const shapeOpacity = Math.pow(shapeFadeProgress, SHAPE_FADE_POWER);
    this.drawShapeLayer(shapeOpacity, canvasWidth, canvasHeight);
  }

  private updateScene() {
    if (this.targetDark === null) {
      return;
    }
    if (this.targetDark) {
      this.spawnInkDrops();
    } else {
      this.spawnLightRays();
    }
  }

  private spawnInkDrops() {
    const spawnWindow = TRANSITION_DURATION_MS * INK_SPAWN_WINDOW_PROGRESS;
    const spawnProgress = clamp(this.timeElapsed / spawnWindow, 0, 1);
    const desiredCount = Math.floor(MAX_INK_DROPS * spawnProgress);
    while (this.inkDrops.length < desiredCount) {
      this.inkDrops.push(this.createInkDrop(this.timeElapsed));
    }
  }

  private spawnLightRays() {
    const spawnWindow = TRANSITION_DURATION_MS * RAY_SPAWN_WINDOW_PROGRESS;
    const spawnProgress = clamp(this.timeElapsed / spawnWindow, 0, 1);
    const desiredCount = Math.floor(MAX_LIGHT_RAYS * spawnProgress);

    while (this.lightRays.length < desiredCount) {
      this.lightRays.push(this.createLightRay(this.timeElapsed));
    }
  }

  private createInkDrop(startTimeMs = this.timeElapsed): InkDrop {
    const width = Math.max(1, this.canvas.clientWidth);
    const height = Math.max(1, this.canvas.clientHeight);
    const diagonal = Math.hypot(width, height);
    const margin = diagonal * 0.12;
    const growthDurationMs = this.remainingTransitionDurationMs(startTimeMs);
    const satellites: InkSatellite[] = [];
    const satelliteCount = 2 + Math.floor(Math.random() * 4);
    for (let i = 0; i < satelliteCount; i++) {
      satellites.push({
        angleRadians: randomBetween(0, Math.PI * 2),
        distanceFactor: randomBetween(0.45, 1.1),
        radiusFactor: randomBetween(0.2, 0.42),
        driftFactor: randomBetween(-0.35, 0.35),
      });
    }

    return {
      x: randomBetween(-margin, width + margin),
      y: randomBetween(-margin, height + margin),
      layerIndex: Math.floor(Math.random() * INK_LAYER_START_COLORS.length),
      startTimeMs,
      growthDurationMs,
      initialRadius: randomBetween(INK_INITIAL_RADIUS_MIN_PX, INK_INITIAL_RADIUS_MAX_PX),
      finalRadius: diagonal,
      satellites,
    };
  }

  private createLightRay(startTimeMs = this.timeElapsed): LightRay {
    const width = Math.max(1, this.canvas.clientWidth);
    const height = Math.max(1, this.canvas.clientHeight);
    const diagonal = Math.hypot(width, height);
    const fromTop = Math.random() < 0.5;
    return {
      startX: fromTop ? randomBetween(width * 0.05, width) : width,
      startY: fromTop ? 0 : randomBetween(0, height * 0.95),
      startColor: LIGHT_RAY_START_COLORS[Math.floor(Math.random() * LIGHT_RAY_START_COLORS.length)],
      angleRadians: LIGHT_RAY_ANGLE_RADIANS,
      startTimeMs,
      growthDurationMs: this.remainingTransitionDurationMs(startTimeMs),
      initialThickness: randomBetween(RAY_INITIAL_THICKNESS_MIN_PX, RAY_INITIAL_THICKNESS_MAX_PX),
      finalThickness: diagonal * randomBetween(0.2, 0.5),
      finalLength: diagonal * LIGHT_RAY_LENGTH_OVERSCAN_MULTIPLIER,
    };
  }

  private spawnInitialShape() {
    if (this.targetDark) {
      this.inkDrops.push(this.createInkDrop(0));
      return;
    }
    this.lightRays.push(this.createLightRay(0));
  }

  private drawInkDrops(ctx: CanvasRenderingContext2D, targetBackgroundColor: string) {
    const layerCircles = INK_LAYER_START_COLORS.map(() => [] as InkCircle[]);
    for (const drop of this.inkDrops) {
      const localProgress = clamp((this.timeElapsed - drop.startTimeMs) / drop.growthDurationMs, 0, 1);
      if (localProgress <= 0) continue;
      const easedGrowth = Math.pow(localProgress, 1.5);
      const coreRadius = lerp(drop.initialRadius, drop.finalRadius, easedGrowth);
      layerCircles[drop.layerIndex].push({ x: drop.x, y: drop.y, radius: coreRadius });
      for (const satellite of drop.satellites) {
        const drift = satellite.driftFactor * easedGrowth;
        const distance = coreRadius * satellite.distanceFactor * (0.7 + easedGrowth * 0.65);
        const radius = coreRadius * satellite.radiusFactor * (0.65 + easedGrowth * 0.35);
        layerCircles[drop.layerIndex].push({
          x: drop.x + Math.cos(satellite.angleRadians + drift) * distance,
          y: drop.y + Math.sin(satellite.angleRadians + drift) * distance,
          radius,
        });
      }
    }

    const colorProgress = clamp(this.timeElapsed / (TRANSITION_DURATION_MS * COLOR_EASE_WINDOW), 0, 1);
    const colorEase = Math.pow(colorProgress, 1.5);
    for (let layerIndex = 0; layerIndex < layerCircles.length; layerIndex++) {
      const circles = layerCircles[layerIndex];
      if (!circles.length) continue;
      ctx.fillStyle = interpolateColor(INK_LAYER_START_COLORS[layerIndex], targetBackgroundColor, colorEase);
      ctx.beginPath();
      for (const circle of circles) {
        if (circle.radius <= 0) continue;
        ctx.moveTo(circle.x + circle.radius, circle.y);
        ctx.arc(circle.x, circle.y, circle.radius, 0, Math.PI * 2);
      }
      ctx.fill();
      // Blend only circles within the same layer.
      this.drawInkMetaballConnectors(ctx, circles);
    }
  }

  private drawLightRays(ctx: CanvasRenderingContext2D, targetBackgroundColor: string) {
    ctx.lineCap = "butt";
    ctx.lineJoin = "miter";
    const diagonal = Math.hypot(Math.max(1, this.canvas.clientWidth), Math.max(1, this.canvas.clientHeight));

    for (const ray of this.lightRays) {
      const localProgress = clamp((this.timeElapsed - ray.startTimeMs) / ray.growthDurationMs, 0, 1);
      if (localProgress <= 0) continue;
      const thicknessGrowth = Math.pow(localProgress, LIGHT_RAY_THICKNESS_GROWTH_POWER);
      const length = Math.max(diagonal, ray.finalLength);
      const thickness = lerp(ray.initialThickness, ray.finalThickness, thicknessGrowth);
      const dirX = Math.cos(ray.angleRadians);
      const dirY = Math.sin(ray.angleRadians);
      const startX = ray.startX - dirX * length;
      const startY = ray.startY - dirY * length;
      const endX = ray.startX + dirX * length;
      const endY = ray.startY + dirY * length;
      ctx.strokeStyle = interpolateColor(ray.startColor, targetBackgroundColor, 1 - Math.pow(1 - localProgress, 2.5));
      ctx.lineWidth = Math.max(1, thickness);
      ctx.beginPath();
      ctx.moveTo(startX, startY);
      ctx.lineTo(endX, endY);
      ctx.stroke();
    }
  }

  private drawInkMetaballConnectors(ctx: CanvasRenderingContext2D, circles: InkCircle[]) {
    const maxConnectionsPerCircle = 7;
    const minRadius = 4;
    const connectionCount = new Array<number>(circles.length).fill(0);

    for (let i = 0; i < circles.length; i++) {
      if (connectionCount[i] >= maxConnectionsPerCircle) continue;
      const a = circles[i];
      if (a.radius < minRadius) continue;
      for (let j = i + 1; j < circles.length; j++) {
        if (connectionCount[i] >= maxConnectionsPerCircle) break;
        if (connectionCount[j] >= maxConnectionsPerCircle) continue;
        const b = circles[j];
        if (b.radius < minRadius) continue;
        if (!this.drawInkMetaballBridge(ctx, a, b)) continue;
        connectionCount[i]++;
        connectionCount[j]++;
      }
    }
  }

  private drawInkMetaballBridge(ctx: CanvasRenderingContext2D, a: InkCircle, b: InkCircle): boolean {
    const maxDistanceMultiplier = 1.45;
    const extraGapRadiusMultiplier = 1.15;
    const viscosity = 0.58;
    const handleLengthRate = 2.2;

    const distance = Math.hypot(b.x - a.x, b.y - a.y);
    if (distance <= 0) return false;
    const radiusDiff = Math.abs(a.radius - b.radius);
    if (distance <= radiusDiff) return false;

    const maxDistanceBySum = (a.radius + b.radius) * maxDistanceMultiplier;
    const maxDistanceByGap = a.radius + b.radius + Math.min(a.radius, b.radius) * extraGapRadiusMultiplier;
    const maxDistance = Math.max(maxDistanceBySum, maxDistanceByGap);
    if (distance > maxDistance) return false;

    const u1 =
      distance < a.radius + b.radius
        ? Math.acos(
            clamp((a.radius * a.radius + distance * distance - b.radius * b.radius) / (2 * a.radius * distance), -1, 1)
          )
        : 0;
    const u2 =
      distance < a.radius + b.radius
        ? Math.acos(
            clamp((b.radius * b.radius + distance * distance - a.radius * a.radius) / (2 * b.radius * distance), -1, 1)
          )
        : 0;

    const angleBetweenCenters = Math.atan2(b.y - a.y, b.x - a.x);
    const angle1 = angleBetweenCenters + u1 + (Math.PI / 2 - u1) * viscosity;
    const angle2 = angleBetweenCenters - u1 - (Math.PI / 2 - u1) * viscosity;
    const angle3 = angleBetweenCenters + Math.PI - u2 - (Math.PI / 2 - u2) * viscosity;
    const angle4 = angleBetweenCenters - Math.PI + u2 + (Math.PI / 2 - u2) * viscosity;

    const p1 = this.pointFromAngle(a.x, a.y, angle1, a.radius);
    const p2 = this.pointFromAngle(a.x, a.y, angle2, a.radius);
    const p3 = this.pointFromAngle(b.x, b.y, angle3, b.radius);
    const p4 = this.pointFromAngle(b.x, b.y, angle4, b.radius);

    const totalRadius = a.radius + b.radius;
    let handleScale = Math.min(viscosity * handleLengthRate, this.pointDistance(p1, p3) / totalRadius);
    handleScale *= Math.min(1, (distance * 2) / totalRadius);
    const handleLenA = a.radius * handleScale;
    const handleLenB = b.radius * handleScale;

    const h1 = this.pointFromVector(p1, angle1 - Math.PI / 2, handleLenA);
    const h2 = this.pointFromVector(p3, angle3 + Math.PI / 2, handleLenB);
    const h3 = this.pointFromVector(p4, angle4 - Math.PI / 2, handleLenB);
    const h4 = this.pointFromVector(p2, angle2 + Math.PI / 2, handleLenA);

    ctx.beginPath();
    ctx.moveTo(p1.x, p1.y);
    ctx.bezierCurveTo(h1.x, h1.y, h2.x, h2.y, p3.x, p3.y);
    ctx.lineTo(p4.x, p4.y);
    ctx.bezierCurveTo(h3.x, h3.y, h4.x, h4.y, p2.x, p2.y);
    ctx.closePath();
    ctx.fill();
    return true;
  }

  private pointFromAngle(cx: number, cy: number, angle: number, radius: number): Point {
    return {
      x: cx + Math.cos(angle) * radius,
      y: cy + Math.sin(angle) * radius,
    };
  }

  private pointFromVector(origin: Point, angle: number, distance: number): Point {
    return {
      x: origin.x + Math.cos(angle) * distance,
      y: origin.y + Math.sin(angle) * distance,
    };
  }

  private pointDistance(a: Point, b: Point): number {
    return Math.hypot(b.x - a.x, b.y - a.y);
  }

  private getShapeLayerContext(width: number, height: number): CanvasRenderingContext2D | null {
    if (!this.shapeLayerCanvas) {
      this.shapeLayerCanvas = document.createElement("canvas");
    }
    const layerWidth = Math.max(1, Math.ceil(width));
    const layerHeight = Math.max(1, Math.ceil(height));
    if (this.shapeLayerCanvas.width !== layerWidth || this.shapeLayerCanvas.height !== layerHeight) {
      this.shapeLayerCanvas.width = layerWidth;
      this.shapeLayerCanvas.height = layerHeight;
    }
    const ctx = this.shapeLayerCanvas.getContext("2d");
    if (!ctx) return null;
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.clearRect(0, 0, layerWidth, layerHeight);
    return ctx;
  }

  private drawShapeLayer(opacity: number, width: number, height: number) {
    if (!this.shapeLayerCanvas || opacity <= 0) {
      return;
    }
    this.ctx.save();
    this.ctx.globalAlpha = opacity;
    this.ctx.drawImage(this.shapeLayerCanvas, 0, 0, width, height);
    this.ctx.restore();
  }

  private remainingTransitionDurationMs(startTimeMs: number) {
    return Math.max(1, TRANSITION_DURATION_MS - startTimeMs);
  }

  private clearScene() {
    this.inkDrops = [];
    this.lightRays = [];
  }

  destroy() {
    this.loop.stop();
    this.clearScene();
  }
}
