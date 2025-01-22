import React from "react";
import NetworkGraph from "./components/NetworkGraph";

const App: React.FC = () => {
  return (
    <div>
      <h1 style={{ textAlign: "center" }}>Network Visualization</h1>
      <NetworkGraph />
    </div>
  );
};

export default App;

