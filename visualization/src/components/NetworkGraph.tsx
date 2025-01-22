import React, { useState, useEffect } from "react";
import Graph from "graphology";
import { cropToLargestConnectedComponent } from "graphology-components";
import forceAtlas2 from "graphology-layout-forceatlas2";
import circular from "graphology-layout/circular";
import Sigma from "sigma";

type TSVRow = Record<string, string>;

const parseTSV = (tsvData: string): { rows: TSVRow[]; headers: string[] } => {
  const [headerLine, ...rows] = tsvData.trim().split("\n");
  const headers = headerLine.split("\t");

  const parsedRows = rows.map((row) => {
    const values = row.split("\t");
    return Object.fromEntries(headers.map((header, i) => [header, values[i]]));
  });

  return { rows: parsedRows, headers };
};

const createGraphFromTSV = (data: TSVRow[], headers: string[]): Graph => {
  const graph = new Graph();

  data.forEach((row) => {
    const nodesInRow = headers.map((col) => row[col]);

    nodesInRow.forEach((node, index) => {
      const nodeType = headers[index];
      if (!graph.hasNode(node)) {
        graph.addNode(node, {
          label: `${nodeType}: ${node}`,
          nodeType: nodeType,
        });
      }
    });

    for (let i = 0; i < nodesInRow.length; i++) {
      for (let j = i + 1; j < nodesInRow.length; j++) {
        const source = nodesInRow[i];
        const target = nodesInRow[j];
        if (!graph.hasEdge(source, target)) {
          graph.addEdge(source, target, { weight: 1 });
        }
      }
    }
  });

  return graph;
};

const visualizeGraph = (graph: Graph, headers: string[], container: HTMLElement) => {
  cropToLargestConnectedComponent(graph);

  const COLORS = headers.reduce((acc, header, index) => {
    const colors = ["#FA5A3D", "#5A75DB", "#FFD700", "#8A2BE2", "#00A676", "#FF6F61"];
    acc[header] = colors[index % colors.length];
    return acc;
  }, {} as Record<string, string>);

  graph.forEachNode((node, attributes) => {
    const nodeType = attributes.nodeType as string;
    const color = COLORS[nodeType] || "#000000";
    graph.setNodeAttribute(node, "color", color);
  });

  // Scale node sizes based on degree
  const degrees = graph.nodes().map((node) => graph.degree(node));
  const minDegree = Math.min(...degrees);
  const maxDegree = Math.max(...degrees);
  const minSize = 5,
    maxSize = 20;

  graph.forEachNode((node) => {
    const degree = graph.degree(node);
    const size = minSize + ((degree - minDegree) / (maxDegree - minDegree)) * (maxSize - minSize);
    graph.setNodeAttribute(node, "size", size);
  });

  circular.assign(graph);
  const settings = forceAtlas2.inferSettings(graph);
  forceAtlas2.assign(graph, { settings, iterations: 600 });

  const renderer = new Sigma(graph, container);

  enableEnhancedDragging(graph, renderer);

  return renderer;
};

const enableEnhancedDragging = (graph: Graph, renderer: Sigma) => {
  let draggedNode: string | null = null;
  let isDragging = false;

  const delta = 6;
  let startX: number;
  let startY: number;
  let allowClick = true;

  renderer.on("downNode", (event) => {
    isDragging = true;
    draggedNode = event.node;
    graph.setNodeAttribute(draggedNode, "highlighted", true);
  });

  renderer.getMouseCaptor().on("mousemovebody", (event) => {
    if (!isDragging || !draggedNode) return;

    const pos = renderer.viewportToGraph(event);

    graph.setNodeAttribute(draggedNode, "x", pos.x);
    graph.setNodeAttribute(draggedNode, "y", pos.y);

    event.preventSigmaDefault();
    event.original.preventDefault();
    event.original.stopPropagation();
  });

  renderer.getMouseCaptor().on("mousedown", (event) => {
    startX = event.original.pageX;
    startY = event.original.pageY;
    if (!renderer.getCustomBBox()) renderer.setCustomBBox(renderer.getBBox());
  });

  renderer.getMouseCaptor().on("mouseup", (event) => {
    if (draggedNode) {
      graph.removeNodeAttribute(draggedNode, "highlighted");

      const diffX = Math.abs(event.original.pageX - startX);
      const diffY = Math.abs(event.original.pageY - startY);

      allowClick = diffX < delta && diffY < delta;

      isDragging = false;
      draggedNode = null;
    }
  });

  renderer.on("clickNode", ({ node }) => {
    if (!graph.getNodeAttribute(node, "hidden") && allowClick) {
      const pageURL = graph.getNodeAttribute(node, "pageURL");
      if (pageURL) window.open(pageURL, "_blank");
    }
  });
};

const NetworkGraph: React.FC = () => {
  const [fileContent, setFileContent] = useState<string | null>(null);

  useEffect(() => {
    if (!fileContent) return;

    let renderer: Sigma | null = null;

    const renderGraph = () => {
      const { rows, headers } = parseTSV(fileContent);
      const graph = createGraphFromTSV(rows, headers);

      const container = document.getElementById("sigma-container");
      if (!container) throw new Error("Sigma container not found.");

      if (renderer) renderer.kill();

      renderer = visualizeGraph(graph, headers, container);
    };

    renderGraph();

    return () => {
      if (renderer) renderer.kill();
    };
  }, [fileContent]);

  const handleFileUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => setFileContent(e.target?.result as string);
    reader.readAsText(file);
  };

  return (
    <div>
      <div
        style={{
          position: "absolute",
          top: 10,
          left: 10,
          zIndex: 1000,
          backgroundColor: "#ffffff",
          padding: "10px",
          border: "1px solid #ddd",
          borderRadius: "4px",
          boxShadow: "0px 4px 6px rgba(0, 0, 0, 0.1)",
        }}
      >
        <label style={{ display: "block", marginBottom: "5px" }}>Upload a TSV File:</label>
        <input
          type="file"
          accept=".tsv"
          onChange={handleFileUpload}
          style={{
            cursor: "pointer",
            padding: "5px",
          }}
        />
      </div>
      <div
        id="sigma-container"
        style={{
          width: "100vw",
          height: "100vh",
          position: "fixed",
          top: 0,
          left: 0,
          backgroundColor: "#f0f0f0",
        }}
      />
    </div>
  );
};

export default NetworkGraph;

