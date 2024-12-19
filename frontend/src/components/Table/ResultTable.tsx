import * as React from 'react'
interface ResultTableProps {
    results: { [key: string]: string };
}

const ResultTable: React.FC<ResultTableProps> = ({ results }) => {

    return (
        <div style={{ display: 'inline-flex', padding: '4px', gap: '4px', border: '1px solid black', backgroundColor: 'rgb(225, 217, 105)'}}>
            {Object.keys(results).map(key => (
                <div key={key} style={{display: 'inline-flex', flexDirection: 'column', gap: '4px', width: '200px', backgroundColor: 'rgb(225, 217, 105)', minWidth: 'min-content', maxWidth: 'max-content'}}>
                    <div style={{ fontWeight: 'bold', border: '1px solid black', textAlign: 'center', backgroundColor: 'rgb(81, 85, 202)', color: 'white', padding: '4px'}}>{key}</div>
                    <div style={{ border: '1px solid black', textAlign: 'center', backgroundColor: 'rgb(232, 160, 232)', padding: '4px'}}>{results[key]}</div>
                </div>))}
        </div>
    );
};

export default ResultTable;